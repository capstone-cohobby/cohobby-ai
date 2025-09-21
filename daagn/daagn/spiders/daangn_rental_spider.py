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
                 query="대여",  # 검색어
                 location_name="노량진동",
                 location_id="6088",
                 max_pages=5,
                 title_keywords="대여|렌탈", # 제목 필수 키워드 (정규식 OR)
                 **kwargs):
        super().__init__(**kwargs)
        # 공백 제거(예: "대 여" → "대여")
        self.query = str(query).replace(" ", "")
        self.location_name = str(location_name)
        self.location_id = str(location_id)
        self.max_pages = int(max_pages)
        self.page = 1
        
        # 제목 필터 정규식 (공백 제거 후 검사)
        self.title_re = re.compile(title_keywords)

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
        
        # (예외 처리) "8/30", "8/31" 같은 달/일 표기는 스킵
        if re.search(r"\d+/\d+", t):
            return "1일"

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
    
    def _parse_deposit(self, text: str) -> str:
        """
        보증금/Deposit 금액 추출
        - "보증금 1.5" → 15000
        - "보증금 2"   → 20000
        - "보증금 2만" → 20000
        - "보증금 15000" → 15000 (그대로)
        """
        deposit_pattern = r"(보[증즈중좁줌좀]금)"
        m = re.search(rf"{deposit_pattern}\s*[:：]?\s*([0-9]+(?:[.,][0-9]+)?)(만|천|원)?", text, re.I)
        if m:
            deposit = self._normalize_amount(m.group(2), m.group(3))
        else:
            deposit = "0"
        return deposit
    
    def _parse_purchase_age(self, text: str) -> str:
        """
        구매시기/연식 추정
        """
        t = text.replace(" ", "")

        # (1) '며칠 전', '며칠전에'
        if re.search(r"며칠전", t):
            return "며칠 전"

        # (2) 'N일/주/개월/년 전'
        m = re.search(r"(\d+)(일|주|개월|달|년)전", t)
        if m:
            return f"{m.group(1)}{m.group(2)} 전"

        # (3) '구매한지 N일/주/개월/년'
        m = re.search(r"(구매|사용)(한지)?(\d+)(일|주|개월|달|년)", t)
        if m:
            return f"{m.group(3)}{m.group(4)}"

        # (4) '작년', '올해'
        if "작년" in t:
            return "작년"
        if "올해" in t or "금년" in t:
            return "올해"

        # (5) '20XX년형/년식'
        m = re.search(r"(20\d{2})년(형|식)?", t)
        if m:
            return f"{m.group(1)}년{m.group(2) or ''}"

        # (6) 모호한 표현
        if re.search(r"(얼마안|거의안|새것같)", t):
            return "최근"

        return ""
    
    def _parse_damage_policy(self, text: str) -> bool:
        """
        손실/파손/수리비 관련 문구 여부
        """
        return bool(re.search(r"(파손|분실|수리비|a/?s|실비|청구)", text, re.I))
    
    def _normalize_amount(self, number: str, unit: str) -> str:
        """
        '1.5만', '2천', '10000원' 같은 문자열을 정수 원 단위 금액으로 변환
        """
        if not number:
            return None

        try:
            value = float(number.replace(",", "").replace(".", ""))  # 기본 숫자 처리
        except ValueError:
            return None

        # 단위 처리
        if unit:
            unit = unit.strip()
            if unit == "만":
                value *= 10000
            elif unit == "천":
                value *= 1000
            elif unit == "원":
                pass  # 그대로
        return str(int(value))


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
        
        # 제목 필터: '대여/렌탈' 등 키워드 없으면 스킵
        title_norm = (product_name or "").replace(" ", "")
        if not self.title_re.search(title_norm):
            self.logger.info(f"[SKIP] title does not match keywords: {product_name}")
            return

        # 대여기간 추출 (기본 1일)
        rental_duration = self._parse_rental_duration(f"{product_name} {desc}")
        
        # item 추출 전에 카테고리 필터링
        if category in ["티켓/교환권", "삽니다", "무료나눔"]:
            self.logger.info(f"[SKIP] excluded category: {category}")
            return
        
        # 가격이 없거나 0원일 때 → 본문에서 대여비/렌탈비 추출
        if not rental_price or rental_price in ["0", "0원", "가격 없음"]:
            m = re.search(r"(대여비|렌탈비|대여)\s*[:：]?\s*([0-9]+(?:[.,][0-9]+)?)(만|천|원)?", desc)
            if m:
                rental_price = self._normalize_amount(m.group(2), m.group(3))
            else:
                rental_price = "0"   
        
        
        # 보증금, 구매 시기 추출
        deposit = self._parse_deposit(desc)
        purchase_age = self._parse_purchase_age(desc)
        damage_policy = self._parse_damage_policy(desc)

        # 결과
        yield {
            "product_name": product_name,
            "rental_price": rental_price,
            "post_link": post_link,
            "category": category,
            "rental_duration": rental_duration or "1일",
            "deposit": deposit or None,
            "purchase_age": purchase_age or None,
            "damage_policy": damage_policy,  # True/False
        }
