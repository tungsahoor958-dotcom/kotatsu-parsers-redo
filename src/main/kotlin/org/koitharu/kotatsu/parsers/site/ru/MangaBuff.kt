package org.koitharu.kotatsu.parsers.site.ru

import kotlinx.coroutines.sync.Mutex
import kotlinx.coroutines.sync.withLock
import okhttp3.Headers
import okhttp3.HttpUrl.Companion.toHttpUrl
import org.jsoup.HttpStatusException
import org.jsoup.Jsoup
import org.jsoup.nodes.Document
import org.jsoup.nodes.Element
import org.koitharu.kotatsu.parsers.MangaLoaderContext
import org.koitharu.kotatsu.parsers.MangaSourceParser
import org.koitharu.kotatsu.parsers.config.ConfigKey
import org.koitharu.kotatsu.parsers.core.PagedMangaParser
import org.koitharu.kotatsu.parsers.exception.ParseException
import org.koitharu.kotatsu.parsers.model.ContentType
import org.koitharu.kotatsu.parsers.model.Manga
import org.koitharu.kotatsu.parsers.model.MangaChapter
import org.koitharu.kotatsu.parsers.model.MangaListFilter
import org.koitharu.kotatsu.parsers.model.MangaListFilterCapabilities
import org.koitharu.kotatsu.parsers.model.MangaListFilterOptions
import org.koitharu.kotatsu.parsers.model.MangaPage
import org.koitharu.kotatsu.parsers.model.MangaParserSource
import org.koitharu.kotatsu.parsers.model.MangaState
import org.koitharu.kotatsu.parsers.model.MangaTag
import org.koitharu.kotatsu.parsers.model.RATING_UNKNOWN
import org.koitharu.kotatsu.parsers.model.SortOrder
import org.koitharu.kotatsu.parsers.model.YEAR_UNKNOWN
import org.koitharu.kotatsu.parsers.util.attrAsAbsoluteUrlOrNull
import org.koitharu.kotatsu.parsers.util.attrAsRelativeUrl
import org.koitharu.kotatsu.parsers.util.attrOrNull
import org.koitharu.kotatsu.parsers.util.generateUid
import org.koitharu.kotatsu.parsers.util.mapChapters
import org.koitharu.kotatsu.parsers.util.parseHtml
import org.koitharu.kotatsu.parsers.util.parseJson
import org.koitharu.kotatsu.parsers.util.parseSafe
import org.koitharu.kotatsu.parsers.util.src
import org.koitharu.kotatsu.parsers.util.toAbsoluteUrl
import org.koitharu.kotatsu.parsers.util.toRelativeUrl
import java.text.SimpleDateFormat
import java.util.EnumSet
import java.util.LinkedHashSet
import java.util.Locale

@MangaSourceParser("MANGABUFF", "MangaBuff", "ru")
internal class MangaBuff(context: MangaLoaderContext) :
	PagedMangaParser(context, MangaParserSource.MANGABUFF, pageSize = 24) {

	override val configKeyDomain = ConfigKey.Domain("mangabuff.ru")

	override fun onCreateConfig(keys: MutableCollection<ConfigKey<*>>) {
		super.onCreateConfig(keys)
		keys.add(userAgentKey)
	}

	override val availableSortOrders: Set<SortOrder> = EnumSet.of(
		SortOrder.POPULARITY,
		SortOrder.UPDATED,
	)

	override val filterCapabilities: MangaListFilterCapabilities
		get() = MangaListFilterCapabilities(
			isSearchSupported = true,
			isSearchWithFiltersSupported = true,
			isMultipleTagsSupported = true,
			isTagsExclusionSupported = true,
			isYearSupported = true,
		)

	@Volatile
	private var csrfToken: String? = null

	@Volatile
	private var filterData: FilterData? = null

	private val filterDataMutex = Mutex()
	private val chapterDateFormat = SimpleDateFormat("dd.MM.yyyy", Locale.ROOT)

	override suspend fun getFilterOptions(): MangaListFilterOptions {
		val data = getFilterData()
		return MangaListFilterOptions(
			availableTags = data.tags,
			availableStates = data.statusMap.keys.ifEmpty {
				EnumSet.of(
					MangaState.ONGOING,
					MangaState.FINISHED,
					MangaState.PAUSED,
					MangaState.ABANDONED,
				)
			},
			availableContentTypes = data.typeMap.keys.ifEmpty {
				EnumSet.of(
					ContentType.MANGA,
					ContentType.MANHWA,
					ContentType.MANHUA,
					ContentType.COMICS,
				)
			},
		)
	}

	override suspend fun getListPage(page: Int, order: SortOrder, filter: MangaListFilter): List<Manga> {
		val query = filter.query.orEmpty().trim()

		if (query.startsWith(SEARCH_PREFIX, ignoreCase = true)) {
			val slug = query.substringAfter(SEARCH_PREFIX).trim('/').trim()
			if (slug.isEmpty()) {
				return emptyList()
			}
			val url = "/manga/$slug"
			val doc = webClient.httpGet(url.toAbsoluteUrl(domain)).parseHtml()
			return listOf(parseDirectMangaCard(doc, url))
		}

		if (query.isNotEmpty()) {
			val url = "https://$domain/search".toHttpUrl().newBuilder().apply {
				addQueryParameter("q", query)
				if (page != 1) {
					addQueryParameter("page", page.toString())
				}
			}.build()
			return parseMangaList(webClient.httpGet(url).parseHtml())
		}

		val data = getFilterData()
		val url = "https://$domain/manga".toHttpUrl().newBuilder().apply {
			filter.tags.forEach { tag ->
				when {
					tag.key.startsWith(TAG_GENRE_PREFIX) -> addQueryParameter("genres[]", tag.key.removePrefix(TAG_GENRE_PREFIX))
					tag.key.startsWith(TAG_TAG_PREFIX) -> addQueryParameter("tags[]", tag.key.removePrefix(TAG_TAG_PREFIX))
					else -> addQueryParameter("genres[]", tag.key)
				}
			}
			filter.tagsExclude.forEach { tag ->
				when {
					tag.key.startsWith(TAG_GENRE_PREFIX) -> addQueryParameter("without_genres[]", tag.key.removePrefix(TAG_GENRE_PREFIX))
					tag.key.startsWith(TAG_TAG_PREFIX) -> addQueryParameter("without_tags[]", tag.key.removePrefix(TAG_TAG_PREFIX))
					else -> addQueryParameter("without_genres[]", tag.key)
				}
			}
			filter.types.forEach { type ->
				data.typeMap[type]?.let { addQueryParameter("type_id[]", it) }
			}
			filter.states.forEach { state ->
				data.statusMap[state]?.let { addQueryParameter("status_id[]", it) }
			}
			if (filter.year != YEAR_UNKNOWN) {
				addQueryParameter("year[]", filter.year.toString())
			}
			addQueryParameter(
				"sort",
				when (order) {
					SortOrder.POPULARITY -> "popular"
					SortOrder.UPDATED -> "latest"
					else -> "latest"
				},
			)
			if (page != 1) {
				addQueryParameter("page", page.toString())
			}
		}.build()
		return parseMangaList(webClient.httpGet(url).parseHtml())
	}

	override suspend fun getDetails(manga: Manga): Manga {
		val doc = webClient.httpGet(manga.url.toAbsoluteUrl(domain)).parseHtml()
		val chapterElements = ArrayList(doc.select(CHAPTER_SELECTOR))

		if (doc.selectFirst(".load-chapters-trigger") != null) {
			val mangaId = doc.selectFirst(".manga")?.attrOrNull("data-id")
			if (!mangaId.isNullOrBlank()) {
				chapterElements.addAll(loadMoreChapterElements(mangaId))
			}
		}

		val stateText = doc.select(".manga__middle-links > a:last-child, .manga-mobile__info > a:last-child").text()
		val altTitles = doc.select(".manga__name-alt > span, .manga-mobile__name-alt > span")
			.eachText()
			.filter { it.isNotBlank() }
			.toSet()

		val rating = doc.selectFirst(".manga__rating")
			?.text()
			?.replace(',', '.')
			?.toFloatOrNull()
			?.div(10f) ?: manga.rating

		return manga.copy(
			title = doc.selectFirst("h1, .manga__name, .manga-mobile__name")?.text().orEmpty().ifBlank { manga.title },
			altTitles = altTitles,
			description = buildDetailsDescription(doc),
			tags = collectTags(doc),
			state = parseStatus(stateText),
			coverUrl = doc.selectFirst(".manga__img img, img.manga-mobile__image")?.src() ?: manga.coverUrl,
			rating = rating,
			chapters = chapterElements.mapChapters(reversed = true) { i, e ->
				val chapterHref = e.attrAsAbsoluteUrlOrNull("href")
					?: e.attrOrNull("href")
					?: throw ParseException("Cannot find chapter href", manga.url.toAbsoluteUrl(domain))
				val chapterUrl = chapterHref.toRelativeUrl(domain)
				val title = e.select(".chapters__volume, .chapters__value, .chapters__name").text()
					.ifBlank { e.text() }
				val chapterNumber = CHAPTER_NUMBER_REGEX.find(title)
					?.groupValues
					?.firstOrNull()
					?.replace(',', '.')
					?.toFloatOrNull()
				MangaChapter(
					id = generateUid(chapterUrl),
					title = title,
					number = chapterNumber ?: (i + 1f),
					volume = 0,
					url = chapterUrl,
					uploadDate = chapterDateFormat.parseSafe(e.selectFirst(".chapters__add-date")?.text()),
					scanlator = null,
					branch = null,
					source = source,
				)
			},
		)
	}

	override suspend fun getPages(chapter: MangaChapter): List<MangaPage> {
		val doc = webClient.httpGet(chapter.url.toAbsoluteUrl(domain)).parseHtml()
		return doc.select(".reader__pages img")
			.mapNotNull { img ->
				val pageUrl = img.attrAsAbsoluteUrlOrNull("data-src")
					?: img.attrAsAbsoluteUrlOrNull("src")
					?: return@mapNotNull null
				MangaPage(
					id = generateUid(pageUrl),
					url = pageUrl.toRelativeUrl(domain),
					preview = null,
					source = source,
				)
			}
	}

	private fun parseMangaList(doc: Document): List<Manga> {
		return doc.select(".cards .cards__item").mapNotNull { item ->
			val a = item.selectFirst("a") ?: return@mapNotNull null
			val href = a.attrAsRelativeUrl("href")
			val slug = href.removeSuffix("/").substringAfterLast('/')
			Manga(
				id = generateUid(href),
				url = href,
				publicUrl = href.toAbsoluteUrl(domain),
				title = item.selectFirst(".cards__name")?.text().orEmpty().ifBlank { a.text().ifBlank { slug } },
				altTitles = emptySet(),
				rating = RATING_UNKNOWN,
				tags = emptySet(),
				authors = emptySet(),
				state = null,
				coverUrl = item.selectFirst("img")?.src() ?: "https://$domain/img/manga/posters/$slug.jpg",
				contentRating = null,
				source = source,
			)
		}
	}

	private fun parseDirectMangaCard(doc: Document, relativeUrl: String): Manga {
		val title = doc.selectFirst("h1, .manga__name, .manga-mobile__name")?.text().orEmpty()
		val cover = doc.selectFirst(".manga__img img, img.manga-mobile__image")?.src()
		val stateText = doc.select(".manga__middle-links > a:last-child, .manga-mobile__info > a:last-child").text()
		return Manga(
			id = generateUid(relativeUrl),
			url = relativeUrl,
			publicUrl = relativeUrl.toAbsoluteUrl(domain),
			title = title.ifBlank { relativeUrl.substringAfterLast('/') },
			altTitles = emptySet(),
			rating = RATING_UNKNOWN,
			tags = emptySet(),
			authors = emptySet(),
			state = parseStatus(stateText),
			coverUrl = cover,
			contentRating = null,
			source = source,
			description = doc.selectFirst(".manga__description")?.text(),
		)
	}

	private fun buildDetailsDescription(doc: Document): String {
		val lines = ArrayList<String>(6)
		doc.selectFirst(".manga__description")?.text()?.takeIf { it.isNotBlank() }?.let(lines::add)

		doc.selectFirst(".manga__rating")?.text()?.replace(',', '.')?.toDoubleOrNull()?.let {
			lines.add("Рейтинг: %.0f%%".format(Locale("ru"), it * 10))
		}
		doc.selectFirst(".manga__views")?.text()?.replace(" ", "")?.toIntOrNull()?.let {
			lines.add("Просмотров: %,d".format(Locale("ru"), it))
		}
		doc.selectFirst(".manga")?.attrOrNull("data-fav-count")?.toIntOrNull()?.let {
			lines.add("Избранное: %,d".format(Locale("ru"), it))
		}
		val alt = doc.select(".manga__name-alt > span, .manga-mobile__name-alt > span")
			.eachText()
			.filter { it.isNotBlank() }
		if (alt.isNotEmpty()) {
			lines.add("Альтернативные названия:")
			lines.addAll(alt.map { "• $it" })
		}
		return lines.joinToString(separator = "\n\n").ifBlank { "" }
	}

	private fun collectTags(doc: Document): Set<MangaTag> {
		val elements = doc.select(
			".manga__middle-links > a:not(:last-child), " +
				".manga-mobile__info > a:not(:last-child), " +
				".tags > .tags__item",
		)
		return elements.mapNotNullTo(LinkedHashSet()) { a ->
			val title = a.text().trim()
			if (title.isEmpty()) {
				return@mapNotNullTo null
			}
			val key = a.attrOrNull("href")
				?.removeSuffix("/")
				?.substringAfterLast('/')
				?.takeIf { it.isNotBlank() }
				?: title.lowercase(Locale.ROOT).replace(Regex("[^\\p{L}\\p{N}]+"), "-").trim('-')
			MangaTag(
				key = key,
				title = title,
				source = source,
			)
		}
	}

	private fun parseStatus(value: String): MangaState? = when (value.lowercase(Locale.ROOT).trim()) {
		"завершен" -> MangaState.FINISHED
		"продолжается" -> MangaState.ONGOING
		"заморожен" -> MangaState.PAUSED
		"заброшен" -> MangaState.ABANDONED
		else -> null
	}

	private suspend fun loadMoreChapterElements(mangaId: String): List<Element> {
		fun headers(token: String): Headers = Headers.Builder()
			.add("X-Requested-With", "XMLHttpRequest")
			.add("X-CSRF-TOKEN", token)
			.build()

		val endpoint = "https://$domain/chapters/load".toHttpUrl()

		return try {
			val json = webClient.httpPost(endpoint, mapOf("manga_id" to mangaId), headers(getToken())).parseJson()
			Jsoup.parseBodyFragment(json.optString("content")).select(CHAPTER_SELECTOR)
		} catch (e: HttpStatusException) {
			if (e.statusCode == 419) {
				csrfToken = null
				val json = webClient.httpPost(endpoint, mapOf("manga_id" to mangaId), headers(getToken())).parseJson()
				Jsoup.parseBodyFragment(json.optString("content")).select(CHAPTER_SELECTOR)
			} else {
				throw e
			}
		}
	}

	private suspend fun getToken(): String {
		csrfToken?.let { return it }
		val url = "https://$domain/"
		val doc = webClient.httpGet(url).parseHtml()
		val token = doc.selectFirst("head meta[name*=csrf-token]")?.attr("content").orEmpty()
		if (token.isEmpty()) {
			throw ParseException("Unable to find CSRF token", url)
		}
		csrfToken = token
		return token
	}

	private suspend fun getFilterData(): FilterData = filterDataMutex.withLock {
		filterData ?: fetchFilterData().also { filterData = it }
	}

	private suspend fun fetchFilterData(): FilterData {
		val doc = webClient.httpGet("https://$domain/manga").parseHtml()

		val tags = LinkedHashSet<MangaTag>()
		// Use only numeric IDs from the real genres <select>.
		doc.select("select[name='genres[]'] option, [name='genres[]'] option").forEach { option ->
			val value = option.attr("value").trim()
			val title = option.text().trim()
			if (value.isNotEmpty() && title.isNotEmpty()) {
				tags.add(
					MangaTag(
						key = TAG_GENRE_PREFIX + value,
						title = title,
						source = source,
					),
				)
			}
		}
		doc.select("select[name='tags[]'] option, [name='tags[]'] option").forEach { option ->
			val value = option.attr("value").trim()
			val title = option.text().trim()
			if (value.isNotEmpty() && title.isNotEmpty()) {
				tags.add(
					MangaTag(
						key = TAG_TAG_PREFIX + value,
						title = title,
						source = source,
					),
				)
			}
		}

		val typeMap = HashMap<ContentType, String>(4)
		doc.select("select[name='type_id[]'] option, [name='type_id[]'] option").forEach { option ->
			val value = option.attr("value").trim()
			val text = option.text().lowercase(Locale.ROOT)
			if (value.isEmpty()) {
				return@forEach
			}
			when {
				"манхва" in text || "manhwa" in text -> typeMap.putIfAbsent(ContentType.MANHWA, value)
				"маньхуа" in text || "manhua" in text -> typeMap.putIfAbsent(ContentType.MANHUA, value)
				"comic" in text || "комик" in text -> typeMap.putIfAbsent(ContentType.COMICS, value)
				"манга" in text || "manga" in text -> typeMap.putIfAbsent(ContentType.MANGA, value)
			}
		}

		val statusMap = HashMap<MangaState, String>(4)
		doc.select("select[name='status_id[]'] option, [name='status_id[]'] option").forEach { option ->
			val value = option.attr("value").trim()
			val text = option.text().lowercase(Locale.ROOT)
			if (value.isEmpty()) {
				return@forEach
			}
			when {
				"продолж" in text || "ongoing" in text -> statusMap.putIfAbsent(MangaState.ONGOING, value)
				"заверш" in text || "completed" in text -> statusMap.putIfAbsent(MangaState.FINISHED, value)
				"заморож" in text || "hiatus" in text -> statusMap.putIfAbsent(MangaState.PAUSED, value)
				"заброш" in text || "dropped" in text -> statusMap.putIfAbsent(MangaState.ABANDONED, value)
			}
		}

		return FilterData(
			tags = tags,
			typeMap = typeMap,
			statusMap = statusMap,
		)
	}

	private data class FilterData(
		val tags: Set<MangaTag>,
		val typeMap: Map<ContentType, String>,
		val statusMap: Map<MangaState, String>,
	)

	private companion object {
		private const val SEARCH_PREFIX = "slug:"
		private const val CHAPTER_SELECTOR = "a.chapters__item"
		private const val TAG_GENRE_PREFIX = "g:"
		private const val TAG_TAG_PREFIX = "t:"
		private val CHAPTER_NUMBER_REGEX = Regex("""\d+(?:[.,]\d+)?""")
	}
}
