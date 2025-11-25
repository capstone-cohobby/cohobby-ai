# scripts/indexer_pdf_chroma.py
"""
로컬 PDF 파일(분쟁조정 사례집)을 읽어와 ChromaDB 'cohobby_dispute' 컬렉션에 색인.
- 선행 조건: pdfplumber 설치 (pip install pdfplumber)
- 실행: poetry run python -m src.hosts.agent.chains.scripts.indexer_pdf_chroma
"""
from __future__ import annotations
import os
import re
import uuid
import glob
import pdfplumber
from typing import List, Dict, Any
from dotenv import load_dotenv

# 기존 emb_store에서 DisputeIndex 가져오기
from src.hosts.agent.stores.emb_store import DisputeIndex

load_dotenv()

# ==========================================
# 1. [2024년 전용] 파서 (형식이 깔끔함)
# ==========================================
def parse_style_2024(lines: List[str], filename: str) -> List[Dict[str, Any]]:
    cases = []
    current_case = None
    
    # 패턴: "1. 제목 (카테고리)"
    pattern = re.compile(r'^\s*(\d+)\.\s+(.*)\((.*)\)\s*$')
    
    # 섹션 감지용 키워드
    section_keywords = ["사건 개요", "당사자 주장", "조정부 판단", "주문", "결정"]

    for line in lines:
        match = pattern.match(line)
        
        # 1. 새 사건 시작
        if match:
            if current_case:
                cases.append(current_case)
            
            case_num = match.group(1)
            title = match.group(2).strip()
            category = match.group(3).strip()
            
            unique_id = f"{filename}_{case_num}_{uuid.uuid4().hex[:4]}"
            
            current_case = {
                "id": unique_id,
                "title": title,
                "category": category,
                "overview": "",
                "arguments": "",
                "judgment": "",
                "source_file": filename
            }
            continue
            
        # 2. 내용 누적 (섹션 구분 없이 통으로 넣거나, 키워드 정도만 체크)
        if current_case:
            # 목차/헤더 제거
            if "CONTENTS" in line or "차례" in line or "제3편" in line:
                continue
            
            # 섹션 헤더는 그냥 본문에 포함시켜서 문맥 유지
            current_case["overview"] += line + " " # 편의상 overview에 다 몰아넣고 나중에 RAG가 알아서 하게 둠

    if current_case:
        cases.append(current_case)
        
    return cases

# ==========================================
# 2. [2021~2023년 전용] 파서 ("사례 1" 형식)
# ==========================================
def parse_style_older(lines: List[str], filename: str) -> List[Dict[str, Any]]:
    cases = []
    current_case = None
    
    # 패턴: "사례 1", "사례 10" (뒤에 제목이 올 수도 있고 아닐 수도 있음)
    case_start_pattern = re.compile(r'^\s*사례\s*(\d+)')
    
    # 목차 제거용 패턴 (숫자로 끝나는 줄은 페이지 번호일 확률 높음)
    toc_pattern = re.compile(r'.*\s+\d+$')

    for i, line in enumerate(lines):
        # 목차(Table of Contents) 페이지 건너뛰기 (단순한 로직)
        if "CONTENTS" in line or "차례" in line:
            continue
        # ...... 50 같은 목차 라인 스킵
        if "..." in line and toc_pattern.match(line):
            continue

        match = case_start_pattern.match(line)
        
        # 1. 새 사건 시작 ("사례 X")
        if match:
            # 진짜 본문인지 확인 (목차에도 '사례 1'이 있을 수 있음)
            # 본문이라면 근처에 '사건 개요'가 있어야 함
            is_real_body = False
            for offset in range(1, 15): # 뒤로 15줄 검사
                if i + offset < len(lines) and "사건" in lines[i+offset] and "개요" in lines[i+offset]:
                    is_real_body = True
                    break
            
            if is_real_body:
                if current_case:
                    cases.append(current_case)
                
                case_num = match.group(1)
                
                # 제목은 보통 '사례 X' 다음 줄이나 그 다음 줄에 옴
                # 간단히 '사례 X' 줄을 포함해서 제목으로 침
                title_candidate = line
                if i + 1 < len(lines):
                    title_candidate += " " + lines[i+1]

                unique_id = f"{filename}_{case_num}_{uuid.uuid4().hex[:4]}"
                
                current_case = {
                    "id": unique_id,
                    "title": title_candidate.strip(),
                    "category": "기타", # 구버전은 카테고리가 제목에 명시 안 된 경우가 많음
                    "overview": "", # 전체 텍스트 통합 저장
                    "arguments": "",
                    "judgment": "",
                    "source_file": filename
                }
                continue

        # 2. 내용 누적
        if current_case:
            # 헤더/푸터 노이즈 제거
            if any(x in line for x in ["전자거래분쟁조정 사례집", "제1편", "제2편", "제3편", "페이지"]):
                continue
            if line.strip().isdigit(): # 페이지 번호
                continue
                
            current_case["overview"] += line + " "

    if current_case:
        cases.append(current_case)
        
    return cases

# ==========================================
# 3. 통합 파서 (파일명 보고 분기)
# ==========================================
def parse_dispatcher(text: str, filename: str) -> List[Dict[str, Any]]:
    lines = [line.strip() for line in text.split('\n') if line.strip()]
    
    if "2024" in filename:
        print(f"[Parser] Using 2024 Style for {filename}")
        return parse_style_2024(lines, filename)
    else:
        print(f"[Parser] Using Older Style (2021~2023) for {filename}")
        return parse_style_older(lines, filename)


# --- 4. PDF 읽기 ---
def extract_text_from_pdf(pdf_path: str) -> str:
    print(f"[Indexer] Reading PDF: {pdf_path}")
    full_text = ""
    try:
        with pdfplumber.open(pdf_path) as pdf:
            for page in pdf.pages:
                text = page.extract_text()
                if text:
                    full_text += text + "\n"
        return full_text
    except Exception as e:
        print(f"[Indexer] PDF Read Error: {e}")
        return ""


# --- 5. 메인 실행 ---
def main():
    DATA_DIR = "./data"
    pdf_files = glob.glob(os.path.join(DATA_DIR, "*.pdf"))
    
    if not pdf_files:
        print(f"[Indexer] '{DATA_DIR}' 폴더 안에 PDF 파일이 없습니다.")
        return

    print(f"[Indexer] Found {len(pdf_files)} PDF files.")

    idx_dispute = DisputeIndex(name="cohobby_dispute")
    
    total_indexed = 0

    for pdf_path in pdf_files:
        filename = os.path.basename(pdf_path)
        print(f"\n--- Processing: {filename} ---")
        
        raw_text = extract_text_from_pdf(pdf_path)
        if not raw_text:
            print(f"[Skip] Failed to extract text.")
            continue

        # [핵심] 파일명에 따라 다른 파서 로직 적용
        cases = parse_dispatcher(raw_text, filename)
        
        # 유효성 검사 (너무 짧으면 스킵)
        valid_cases = [c for c in cases if len(c["overview"]) > 50]
        
        if not valid_cases:
            print(f"[Skip] 0 valid cases found.")
            continue
            
        print(f"[Indexer] Parsed {len(valid_cases)} valid cases.")

        idx_dispute.upsert_cases(valid_cases)
        total_indexed += len(valid_cases)
        print(f"[Indexer] Saved batch to DB.")

    print(f"\n[Done] 총 {len(pdf_files)}개 파일 처리 완료. 총 {total_indexed}건의 사례 저장됨.")

if __name__ == "__main__":
    main()