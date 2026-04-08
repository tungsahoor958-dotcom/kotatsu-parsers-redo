package org.koitharu.kotatsu.parsers.site.all

import okhttp3.Headers
import okhttp3.Interceptor
import okhttp3.Response
import org.json.JSONArray
import org.json.JSONObject
import org.jsoup.HttpStatusException
import org.koitharu.kotatsu.parsers.MangaLoaderContext
import org.koitharu.kotatsu.parsers.MangaSourceParser
import org.koitharu.kotatsu.parsers.config.ConfigKey
import org.koitharu.kotatsu.parsers.core.PagedMangaParser
import org.koitharu.kotatsu.parsers.model.*
import org.koitharu.kotatsu.parsers.util.*
import org.koitharu.kotatsu.parsers.util.json.getLongOrDefault
import org.koitharu.kotatsu.parsers.util.json.getStringOrNull
import org.koitharu.kotatsu.parsers.util.json.mapJSONNotNull
import java.text.SimpleDateFormat
import java.util.EnumSet
import java.util.Locale
import java.util.TimeZone

@MangaSourceParser("KEMONOCR", "Kemono", type = ContentType.OTHER)
internal class KemonoCrParser(context: MangaLoaderContext) :
    PagedMangaParser(context, MangaParserSource.KEMONOCR, pageSize = 50) {

    override val configKeyDomain = ConfigKey.Domain("kemono.cr")

    private val dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).apply {
        timeZone = TimeZone.getTimeZone("UTC")
    }

    private val modePostsTag by lazy { MangaTag(title = "Posts", key = "posts", source = source) }
    private val modeUsersTag by lazy { MangaTag(title = "Users", key = "users", source = source) }

    @Volatile
    private var creatorsCache: List<Creator>? = null

    override fun onCreateConfig(keys: MutableCollection<ConfigKey<*>>) {
        super.onCreateConfig(keys)
        keys.add(userAgentKey)
    }

    override fun getRequestHeaders(): Headers = super.getRequestHeaders()
        .newBuilder()
        .set("Accept", "text/css")
        .build()

    override fun intercept(chain: Interceptor.Chain): Response {
        val request = chain.request().newBuilder()
            .header("Accept", "text/css")
            .build()
        return chain.proceed(request)
    }

    override val availableSortOrders: Set<SortOrder> = EnumSet.of(SortOrder.NEWEST, SortOrder.UPDATED)

    override val filterCapabilities: MangaListFilterCapabilities
        get() = MangaListFilterCapabilities(
            isSearchSupported = true,
            isSearchWithFiltersSupported = true,
        )

    override suspend fun getFilterOptions() = MangaListFilterOptions(
        availableTags = setOf(modePostsTag, modeUsersTag),
    )

    override suspend fun getListPage(page: Int, order: SortOrder, filter: MangaListFilter): List<Manga> {
        return when (modeFromFilter(filter)) {
            ListMode.POSTS -> getPostsListPage(page = page, filter = filter)
            ListMode.USERS -> getUsersListPage(page = page, order = order, filter = filter)
        }
    }

    override suspend fun getDetails(manga: Manga): Manga {
        PostRef.from(manga.url)?.let { postRef ->
            return getPostDetails(manga = manga, postRef = postRef)
        }
        UserRef.from(manga.url)?.let { userRef ->
            return getUserDetails(manga = manga, userRef = userRef)
        }
        return manga
    }

    override suspend fun getPages(chapter: MangaChapter): List<MangaPage> {
        val postRef = PostRef.from(chapter.url) ?: return emptyList()
        val details = webClient.httpGet(postRef.toApiUrl(domain)).parseJson()
        return extractImageUrls(details).mapIndexed { index, url ->
            MangaPage(
                id = generateUid("${chapter.id}:$index:$url"),
                url = url,
                preview = null,
                source = source,
            )
        }
    }

    private suspend fun getPostsListPage(page: Int, filter: MangaListFilter): List<Manga> {
        val offset = (page - 1) * pageSize
        val url = buildString {
            append("https://")
            append(domain)
            append("/api/v1/posts?o=")
            append(offset)
            filter.query?.takeIf { it.isNotBlank() }?.let {
                append("&q=")
                append(it.urlEncoded())
            }
        }
        val root = webClient.httpGet(url).parseJson()
        val posts = root.optJSONArray("posts") ?: JSONArray()
        return posts.mapJSONNotNull { it.toPostMangaOrNull() }
    }

    private suspend fun getUsersListPage(page: Int, order: SortOrder, filter: MangaListFilter): List<Manga> {
        val offset = (page - 1) * pageSize
        val query = filter.query?.trim().orEmpty()

        val sorted = getCreators()
            .asSequence()
            .filter { creator ->
                query.isEmpty() ||
                    creator.name.contains(query, ignoreCase = true) ||
                    creator.user.contains(query, ignoreCase = true)
            }
            .sortedWith(
                when (order) {
                    SortOrder.UPDATED -> compareByDescending<Creator> { it.updated }
                        .thenByDescending { it.indexed }
                    else -> compareByDescending<Creator> { it.indexed }
                        .thenByDescending { it.updated }
                },
            )
            .drop(offset)
            .take(pageSize)
            .toList()

        return sorted.map { it.toUserManga() }
    }

    private suspend fun getPostDetails(manga: Manga, postRef: PostRef): Manga {
        val details = webClient.httpGet(postRef.toApiUrl(domain)).parseJson()
        val post = details.optJSONObject("post") ?: return manga
        val creatorName = fetchCreatorName(postRef)
        val publishedAt = dateFormat.parseSafe(post.getStringOrNull("published"))
        val description = post.getStringOrNull("content")
            .ifNullOrEmpty { post.getStringOrNull("substring").orEmpty() }
            .trim()
            .nullIfEmpty()
        val imageUrls = extractImageUrls(details)

        return manga.copy(
            title = post.getStringOrNull("title").ifNullOrEmpty { manga.title },
            coverUrl = imageUrls.firstOrNull() ?: manga.coverUrl,
            description = description,
            authors = setOfNotNull(creatorName),
            tags = setOf(
                MangaTag(
                    title = postRef.service.toTitleCase(),
                    key = postRef.service,
                    source = source,
                ),
            ),
            chapters = listOf(
                MangaChapter(
                    id = generateUid(postRef.toKey()),
                    title = post.getStringOrNull("title"),
                    number = 1f,
                    volume = 0,
                    url = manga.url,
                    scanlator = creatorName,
                    uploadDate = publishedAt,
                    branch = null,
                    source = source,
                ),
            ),
        )
    }

    private suspend fun getUserDetails(manga: Manga, userRef: UserRef): Manga {
        val profile = runCatchingCancellable {
            webClient.httpGet(userRef.toProfileApiUrl(domain)).parseJson()
        }.getOrNull()

        val creatorName = profile?.getStringOrNull("name").ifNullOrEmpty { manga.title }
        val posts = fetchAllUserPosts(userRef)

        val chapters = posts.mapIndexed { index, post ->
            val postRef = PostRef(
                service = userRef.service,
                user = userRef.user,
                postId = post.postId,
            )
            MangaChapter(
                id = generateUid(postRef.toKey()),
                title = post.title.ifNullOrEmpty { "Post #${post.postId}" },
                number = (posts.size - index).toFloat(),
                volume = 0,
                url = postRef.toPublicPath(),
                scanlator = creatorName,
                uploadDate = dateFormat.parseSafe(post.published),
                branch = null,
                source = source,
            )
        }

        return manga.copy(
            title = creatorName,
            coverUrl = manga.coverUrl.ifNullOrEmpty { userRef.toIconUrl() },
            authors = setOfNotNull(creatorName),
            tags = setOf(
                MangaTag(
                    title = userRef.service.toTitleCase(),
                    key = userRef.service,
                    source = source,
                ),
            ),
            chapters = chapters,
        )
    }

    private suspend fun fetchAllUserPosts(userRef: UserRef): List<UserPost> {
        val posts = ArrayList<UserPost>(pageSize)
        var offset = 0
        while (true) {
            val page = try {
                webClient.httpGet(userRef.toPostsApiUrl(domain, offset)).parseJsonArray()
            } catch (e: HttpStatusException) {
                if (e.statusCode == 400 && offset > 0) {
                    break
                }
                throw e
            }

            if (page.length() == 0) {
                break
            }

            posts.addAll(page.mapJSONNotNull { it.toUserPostOrNull() })

            if (page.length() < pageSize) {
                break
            }
            offset += pageSize
        }
        return posts
    }

    private suspend fun getCreators(): List<Creator> {
        creatorsCache?.let { return it }
        val creators = webClient.httpGet("https://$domain/api/v1/creators")
            .parseJsonArray()
            .mapJSONNotNull { it.toCreatorOrNull() }
        creatorsCache = creators
        return creators
    }

    private suspend fun fetchCreatorName(postRef: PostRef): String? = runCatchingCancellable {
        webClient.httpGet(postRef.toProfileApiUrl(domain)).parseJson().getStringOrNull("name")
    }.getOrNull()

    private fun modeFromFilter(filter: MangaListFilter): ListMode {
        return when (filter.tags.firstOrNull()?.key) {
            modeUsersTag.key -> ListMode.USERS
            else -> ListMode.POSTS
        }
    }

    private fun JSONObject.toPostMangaOrNull(): Manga? {
        val service = getStringOrNull("service") ?: return null
        val user = getStringOrNull("user") ?: return null
        val postId = getStringOrNull("id") ?: return null
        val postRef = PostRef(service = service, user = user, postId = postId)
        val title = getStringOrNull("title").ifNullOrEmpty { "Post #$postId" }
        val coverPath = extractCoverPath()
        return Manga(
            id = generateUid(postRef.toKey()),
            title = title,
            altTitles = emptySet(),
            url = postRef.toPublicPath(),
            publicUrl = postRef.toPublicUrl(domain),
            rating = RATING_UNKNOWN,
            contentRating = null,
            coverUrl = coverPath?.toDataUrl(),
            tags = emptySet(),
            state = null,
            authors = emptySet(),
            source = source,
        )
    }

    private fun JSONObject.toCreatorOrNull(): Creator? {
        val service = getStringOrNull("service") ?: return null
        val user = getStringOrNull("id") ?: return null
        val name = getStringOrNull("name").ifNullOrEmpty { user }
        return Creator(
            service = service,
            user = user,
            name = name,
            indexed = getLongOrDefault("indexed", 0L),
            updated = getLongOrDefault("updated", 0L),
        )
    }

    private fun Creator.toUserManga(): Manga {
        val userRef = UserRef(service = service, user = user)
        return Manga(
            id = generateUid(userRef.toKey()),
            title = name,
            altTitles = emptySet(),
            url = userRef.toPublicPath(),
            publicUrl = userRef.toPublicUrl(domain),
            rating = RATING_UNKNOWN,
            contentRating = null,
            coverUrl = userRef.toIconUrl(),
            tags = setOf(
                MangaTag(
                    title = service.toTitleCase(),
                    key = service,
                    source = source,
                ),
            ),
            state = null,
            authors = setOf(name),
            source = source,
        )
    }

    private fun JSONObject.toUserPostOrNull(): UserPost? {
        val postId = getStringOrNull("id") ?: return null
        return UserPost(
            postId = postId,
            title = getStringOrNull("title"),
            published = getStringOrNull("published"),
        )
    }

    private fun JSONObject.extractCoverPath(): String? {
        optJSONObject("file")
            ?.getStringOrNull("path")
            ?.takeIf(::isImagePath)
            ?.let { return it }
        val attachments = optJSONArray("attachments") ?: return null
        for (i in 0 until attachments.length()) {
            val path = attachments.optJSONObject(i)?.getStringOrNull("path") ?: continue
            if (isImagePath(path)) {
                return path
            }
        }
        return null
    }

    private fun extractImageUrls(details: JSONObject): List<String> {
        // Keep unique image path entries and prefer direct media-server URLs over domain redirects.
        val urlsByPath = linkedMapOf<String, String>()

        details.optJSONArray("attachments")?.let { attachments ->
            for (i in 0 until attachments.length()) {
                val jo = attachments.optJSONObject(i) ?: continue
                addImagePath(
                    urlsByPath = urlsByPath,
                    path = jo.getStringOrNull("path"),
                    server = jo.getStringOrNull("server"),
                )
            }
        }

        details.optJSONObject("post")?.let { post ->
            addImagePath(urlsByPath, post.optJSONObject("file")?.getStringOrNull("path"), null)
            val postAttachments = post.optJSONArray("attachments")
            if (postAttachments != null) {
                for (i in 0 until postAttachments.length()) {
                    val jo = postAttachments.optJSONObject(i) ?: continue
                    addImagePath(urlsByPath, jo.getStringOrNull("path"), null)
                }
            }
        }

        // Preview entries are usually duplicates/thumbnails; use only as fallback when no full images exist.
        if (urlsByPath.isEmpty()) {
            details.optJSONArray("previews")?.let { previews ->
                for (i in 0 until previews.length()) {
                    val jo = previews.optJSONObject(i) ?: continue
                    addImagePath(
                        urlsByPath = urlsByPath,
                        path = jo.getStringOrNull("path"),
                        server = jo.getStringOrNull("server"),
                    )
                }
            }
        }

        return urlsByPath.values.toList()
    }

    private fun addImagePath(urlsByPath: MutableMap<String, String>, path: String?, server: String?) {
        if (path.isNullOrEmpty() || !isImagePath(path)) {
            return
        }
        val normalizedPath = if (path.startsWith('/')) path else "/$path"
        val pathKey = normalizedPath.substringBefore('?').substringBefore('#').lowercase(Locale.ROOT)

        val fullUrl = if (!server.isNullOrEmpty()) {
            "${server.removeSuffix('/')}/data$normalizedPath"
        } else {
            normalizedPath.toDataUrl()
        }

        val existing = urlsByPath[pathKey]
        if (existing == null) {
            urlsByPath[pathKey] = fullUrl
            return
        }

        // Replace slower domain redirect URLs with direct media-server URLs when available.
        if (!server.isNullOrEmpty() && existing.startsWith("https://$domain/data")) {
            urlsByPath[pathKey] = fullUrl
        }
    }

    private fun String.toDataUrl(): String {
        val normalizedPath = if (startsWith('/')) this else "/$this"
        return "https://$domain/data$normalizedPath"
    }

    private fun isImagePath(path: String): Boolean {
        val normalized = path.substringBefore('?').substringBefore('#').lowercase(Locale.ROOT)
        return normalized.endsWith(".jpg") ||
            normalized.endsWith(".jpeg") ||
            normalized.endsWith(".png") ||
            normalized.endsWith(".webp") ||
            normalized.endsWith(".avif") ||
            normalized.endsWith(".bmp")
    }

    private enum class ListMode {
        POSTS,
        USERS,
    }

    private data class Creator(
        val service: String,
        val user: String,
        val name: String,
        val indexed: Long,
        val updated: Long,
    )

    private data class UserPost(
        val postId: String,
        val title: String?,
        val published: String?,
    )

    private data class UserRef(
        val service: String,
        val user: String,
    ) {
        fun toKey(): String = "creator:$service:$user"

        fun toPublicPath(): String = "/$service/user/$user"

        fun toPublicUrl(domain: String): String = "https://$domain${toPublicPath()}"

        fun toProfileApiUrl(domain: String): String = "https://$domain/api/v1/$service/user/$user/profile"

        fun toPostsApiUrl(domain: String, offset: Int): String {
            return "https://$domain/api/v1/$service/user/$user/posts?o=$offset"
        }

        fun toIconUrl(): String = "https://img.kemono.cr/icons/$service/$user"

        companion object {
            private val routeRegex = Regex("""/?([^/]+)/user/([^/?#]+)/?${'$'}""")

            fun from(url: String): UserRef? {
                val match = routeRegex.find(url) ?: return null
                return UserRef(
                    service = match.groupValues[1],
                    user = match.groupValues[2],
                )
            }
        }
    }

    private data class PostRef(
        val service: String,
        val user: String,
        val postId: String,
    ) {
        fun toKey(): String = "$service:$user:$postId"

        fun toPublicPath(): String = "/$service/user/$user/post/$postId"

        fun toPublicUrl(domain: String): String = "https://$domain${toPublicPath()}"

        fun toApiUrl(domain: String): String = "https://$domain/api/v1/$service/user/$user/post/$postId"

        fun toProfileApiUrl(domain: String): String = "https://$domain/api/v1/$service/user/$user/profile"

        companion object {
            private val routeRegex = Regex("""/?([^/]+)/user/([^/]+)/post/([^/?#]+)""")

            fun from(url: String): PostRef? {
                val match = routeRegex.find(url) ?: return null
                return PostRef(
                    service = match.groupValues[1],
                    user = match.groupValues[2],
                    postId = match.groupValues[3],
                )
            }
        }
    }
}
