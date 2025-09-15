import re
import json
from urllib.parse import urlencode, urljoin, urlparse, parse_qsl
import scrapy


class DaangnRentalSpider(scrapy.Spider):
    name = "daangn_rental"

    # 상세 URL 패턴: /kr/buy-sell/<슬러그>-<id>
    DETAIL_RE = re.compile(r"/kr/buy-sell/[^/?#]*-[a-z0-9]{6,}", re.I)

    custom_settings = {
        "DEFAULT_REQUEST_HEADERS": {
            "User-Agent": (
                "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
                "AppleWebKit/537.36 (KHTML, like Gecko) "
                "Chrome/120.0.0.0 Safari/537.36"
            ),
            "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
        },
        "ROBOTSTXT_OBEY": False,   # 주의: 학습/개인용 소량 수집에서만 권장
        "CONCURRENT_REQUESTS": 1,
        "DOWNLOAD_DELAY": 1,
        "FEED_EXPORT_ENCODING": "utf-8",
    }

    def __init__(self,
                 query="대여",
                 location_name="노량진동",
                 location_id="6088",
                 max_pages=5,
                 **kwargs):
        super().__init__(**kwargs)
        # 공백 제거(예: "대 여" → "대여")
        self.query = str(query).replace(" ", "")
        self.location_name = str(location_name)
        self.location_id = str(location_id)
        self.max_pages = int(max_pages)
        self.page = 1

        base = "https://www.daangn.com/kr/buy-sell/"
        qs = {
            "in": f"{self.location_name}-{self.location_id}",
            "search": self.query,
            "page": str(self.page),
        }
        self.start_urls = [f"{base}?{urlencode(qs, safe='-')}"]

    # ---------- 공통 유틸 ----------
    def _clean_text(self, s: str) -> str:
        return re.sub(r"\s+", " ", s or "").strip()

    def _parse_rental_duration(self, text: str) -> str:
        """본문/제목에서 대여 기간 추정. 실패 시 '1일'."""
        if not text:
            return "1일"
        t = text.replace(" ", "")

        m = re.search(r"(\d+)\s*박\s*(\d+)\s*일", t)   # 2박3일
        if m:
            return f"{m.group(2)}일"

        m = re.search(r"(\d+)(?:\s*[-~]\s*\d+)?\s*일", t)  # 1일, 2~3일
        if m:
            return f"{m.group(1)}일"

        if re.search(r"하루|당일", t):
            return "1일"

        m = re.search(r"(\d+)\s*주(?:일)?", t)  # 1주
        if m:
            return f"{int(m.group(1)) * 7}일"

        m = re.search(r"(\d+)\s*시간", t)  # 6시간, 24시간
        if m:
            hours = int(m.group(1))
            return f"{hours}시간" if hours < 24 else f"{max(1, hours // 24)}일"

        if re.search(r"(한달|1개월|30일)", t):
            return "30일"

        return "1일"

    def _guess_category(self, response) -> str:
        """페이지 내 요소/메타에서 카테고리 추정."""
        candidates = [
            response.css('[class*="breadcrumb"] a::text').getall(),
            response.css('[class*="category"]::text').getall(),
            response.css('a[href*="category"]::text').getall(),
        ]
        texts = [self._clean_text(x) for sub in candidates for x in (sub or []) if self._clean_text(x)]
        if texts:
            return texts[-1]

        meta_cat = response.css(
            'meta[property="article:section"]::attr(content), '
            'meta[property="article:tag"]::attr(content)'
        ).get()
        if meta_cat:
            return self._clean_text(meta_cat)
        return ""

    def _extract_post_link(self, response) -> str:
        link = response.css('link[rel="canonical"]::attr(href)').get()
        if link:
            return link
        link = response.css('meta[property="og:url"]::attr(content)').get()
        return link or response.url

    def _page_url(self, url: str, page: int) -> str:
        pu = urlparse(url)
        q = dict(parse_qsl(pu.query, keep_blank_values=True))
        q["page"] = str(page)
        return f"{pu.scheme}://{pu.netloc}{pu.path}?{urlencode(q, safe='-')}"

    # ---------- 리스트 파서 ----------
    def parse(self, response):
        # 1) 흔한 a[href]들
        hrefs = response.css(
            'a[href*="/kr/buy-sell/"]::attr(href), '
            '[data-testid*="card"] a::attr(href), '
            'a[aria-label*="게시글"]::attr(href)'
        ).getall()

        # 2) 전역 a 태그
        if not hrefs:
            hrefs = response.css('a::attr(href)').getall()

        # 3) 정규식 회수(클라 렌더링 대비)
        if not hrefs:
            hrefs = self.DETAIL_RE.findall(response.text)

        # 정규화 + 필터링
        seen = set()
        detail_urls = []
        for h in hrefs:
            if not self.DETAIL_RE.search(h):  # 상세 패턴만 통과
                continue
            clean = h.split("?")[0]
            if not clean.startswith("http"):
                clean = urljoin(response.url, clean)
            if clean not in seen:
                seen.add(clean)
                detail_urls.append(clean)

        self.logger.info(f"[LIST] found detail links: {len(detail_urls)}")
        for u in detail_urls:
            yield scrapy.Request(u, callback=self.parse_detail)

        # 페이지네이션
        self.page += 1
        if self.page <= self.max_pages:
            yield response.follow(self._page_url(response.url, self.page), callback=self.parse)

    # ---------- 상세 파서 ----------
    def parse_detail(self, response):
        def clean(s: str) -> str:
            return re.sub(r"\s+", " ", (s or "")).strip()

        def pick_first(*vals):
            for v in vals:
                if v and clean(v):
                    return clean(v)
            return ""

        # (A) 메타 태그
        og_title = response.css('meta[property="og:title"]::attr(content)').get()
        og_url = response.css('meta[property="og:url"]::attr(content)').get()
        canonical = response.css('link[rel="canonical"]::attr(href)').get()
        meta_price = response.css('meta[property="product:price:amount"]::attr(content)').get()
        article_section = response.css('meta[property="article:section"]::attr(content)').get()

        # (B) JSON-LD(Product)
        ld_name = ld_price = ld_category = ""
        for node in response.css('script[type="application/ld+json"]::text').getall():
            try:
                data = json.loads(node)
            except Exception:
                continue
            candidates = data if isinstance(data, list) else [data]
            for d in candidates:
                if isinstance(d, dict) and d.get("@type") in ("Product", "Offer"):
                    if not ld_name:
                        ld_name = d.get("name") or (d.get("itemOffered") or {}).get("name")
                    offers = d.get("offers") or {}
                    if isinstance(offers, list):
                        offers = offers[0] if offers else {}
                    if not ld_price:
                        ld_price = str(
                            offers.get("price") or
                            (offers.get("priceSpecification") or {}).get("price") or ""
                        )
                    if not ld_category:
                        ld_category = d.get("category") or (d.get("itemOffered") or {}).get("category")

        # (C) 화면 텍스트 보강
        h1_title = response.css('h1::text, [data-testid*="title"]::text, #article-title::text').get()
        price_text = response.css(
            '[class*="price"]::text, #article-price::text, #article-price-nanum::text'
        ).get()

        cat_texts = response.css(
            'nav[aria-label="breadcrumb"] a::text, [class*="breadcrumb"] a::text, '
            '[class*="category"]::text, a[href*="category"]::text'
        ).getall()
        cat_texts = [clean(x) for x in cat_texts if clean(x)]
        cat_guess = cat_texts[-1] if cat_texts else ""

        desc_parts = response.css('#article-detail *::text, [class*="content"] *::text, article *::text').getall()
        desc = clean(" ".join([p for p in desc_parts if clean(p)]))

        # 최종 합치기
        product_name = pick_first(og_title, ld_name, h1_title)
        rental_price = pick_first(meta_price, ld_price, price_text)
        post_link = pick_first(canonical, og_url, response.url)
        category = pick_first(ld_category, article_section, cat_guess)

        # 대여기간 추출 (기본 1일)
        rental_duration = self._parse_rental_duration(f"{product_name} {desc}")

        # 결과
        yield {
            "product_name": product_name,
            "rental_price": rental_price,
            "post_link": post_link,
            "category": category,
            "rental_duration": rental_duration or "1일",
        }
