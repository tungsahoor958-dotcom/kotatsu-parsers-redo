package org.koitharu.kotatsu.parsers.site.all

import okhttp3.Headers
import okhttp3.Interceptor
import okhttp3.Response
import org.json.JSONArray
import org.json.JSONObject
import org.koitharu.kotatsu.parsers.MangaLoaderContext
import org.koitharu.kotatsu.parsers.MangaSourceParser
import org.koitharu.kotatsu.parsers.config.ConfigKey
import org.koitharu.kotatsu.parsers.core.PagedMangaParser
import org.koitharu.kotatsu.parsers.model.*
import org.koitharu.kotatsu.parsers.util.*
import org.koitharu.kotatsu.parsers.util.json.getStringOrNull
import org.koitharu.kotatsu.parsers.util.json.mapJSONNotNull
import java.text.SimpleDateFormat
import java.util.EnumSet
import java.util.LinkedHashSet
import java.util.Locale
import java.util.TimeZone

@MangaSourceParser("KEMONOCR", "Kemono", type = ContentType.OTHER)
internal class KemonoCrParser(context: MangaLoaderContext) :
    PagedMangaParser(context, MangaParserSource.KEMONOCR, pageSize = 50) {

    override val configKeyDomain = ConfigKey.Domain("kemono.cr")

    private val dateFormat = SimpleDateFormat("yyyy-MM-dd'T'HH:mm:ss", Locale.US).apply {
        timeZone = TimeZone.getTimeZone("UTC")
    }

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
        )

    override suspend fun getFilterOptions() = MangaListFilterOptions()

    override suspend fun getListPage(page: Int, order: SortOrder, filter: MangaListFilter): List<Manga> {
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
        return posts.mapJSONNotNull { it.toMangaOrNull() }
    }

    override suspend fun getDetails(manga: Manga): Manga {
        val postRef = PostRef.from(manga.url) ?: return manga
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
                    id = generateUid("${postRef.service}:${postRef.user}:${postRef.postId}"),
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

    private suspend fun fetchCreatorName(postRef: PostRef): String? = runCatchingCancellable {
        webClient.httpGet(postRef.toProfileApiUrl(domain)).parseJson().getStringOrNull("name")
    }.getOrNull()

    private fun JSONObject.toMangaOrNull(): Manga? {
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
        val urls = LinkedHashSet<String>()
        details.optJSONObject("post")?.let { post ->
            addImagePath(urls, post.optJSONObject("file")?.getStringOrNull("path"), null)
            val postAttachments = post.optJSONArray("attachments")
            if (postAttachments != null) {
                for (i in 0 until postAttachments.length()) {
                    val jo = postAttachments.optJSONObject(i) ?: continue
                    addImagePath(urls, jo.getStringOrNull("path"), null)
                }
            }
        }
        details.optJSONArray("attachments")?.let { attachments ->
            for (i in 0 until attachments.length()) {
                val jo = attachments.optJSONObject(i) ?: continue
                addImagePath(
                    urls = urls,
                    path = jo.getStringOrNull("path"),
                    server = jo.getStringOrNull("server"),
                )
            }
        }
        details.optJSONArray("previews")?.let { previews ->
            for (i in 0 until previews.length()) {
                val jo = previews.optJSONObject(i) ?: continue
                addImagePath(
                    urls = urls,
                    path = jo.getStringOrNull("path"),
                    server = jo.getStringOrNull("server"),
                )
            }
        }
        return urls.toList()
    }

    private fun addImagePath(urls: LinkedHashSet<String>, path: String?, server: String?) {
        if (path.isNullOrEmpty() || !isImagePath(path)) {
            return
        }
        val normalizedPath = if (path.startsWith('/')) path else "/$path"
        val fullUrl = if (!server.isNullOrEmpty()) {
            "${server.removeSuffix('/')}/data$normalizedPath"
        } else {
            normalizedPath.toDataUrl()
        }
        urls.add(fullUrl)
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
