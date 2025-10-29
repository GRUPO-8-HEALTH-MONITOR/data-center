import re
import csv
import argparse
import pdfplumber
from collections import defaultdict, deque

CID_PATTERN = re.compile(r"\b[A-Z][0-9]{2}(?:\.[0-9]+)?\b")
MEDICINE_HINTS = [
    "mg",
    "mcg",
    "ui",
    "ml",
    "comprimido",
    "capsula",
    "cápsula",
    "inject",
    "injet",
    "injetável",
    "frasco",
    "ampola",
    "ampola",
    "seringa",
    "lar",
    "grupo",
    "apac",
    "resolução",
    "resolucao",
]


def is_medicine_line(text: str) -> bool:
    t = text.lower()
    if any(h in t for h in MEDICINE_HINTS):
        return True
    if re.search(r"\b\d+[\.,]?\d*\s*(mg|mcg|ui|ml)\b", t):
        return True
    if "grupo" in t or "apac" in t:
        return True
    return False


def extract_disease_medicine(pdf_path: str) -> dict:
    data = defaultdict(set)
    current_disease = None
    buffer = deque(maxlen=8)

    with pdfplumber.open(pdf_path) as pdf:
        for page in pdf.pages:
            text = page.extract_text()
            if not text:
                continue
            for raw_line in text.splitlines():
                line = raw_line.strip()
                if not line:
                    continue
                buffer.append(line)

                cid_matches = list(CID_PATTERN.finditer(line))
                if cid_matches:
                    first = cid_matches[0]
                    disease_candidate = line[: first.start()].strip()

                    if not disease_candidate:
                        for prev in reversed(list(buffer)[:-1]):
                            if not CID_PATTERN.search(prev) and not is_medicine_line(prev):
                                disease_candidate = prev.strip()
                                break

                    disease_candidate = re.sub(r"[;,:\-\s]+$", "", disease_candidate).strip()
                    disease_candidate = re.sub(r"\s+", " ", disease_candidate)
                    disease_candidate = disease_candidate.strip(';,')

                    current_disease = disease_candidate if disease_candidate else None

                    after = line[cid_matches[-1].end() :].strip()
                    if after and is_medicine_line(after):
                        med = re.sub(r"\s+", " ", after)
                        data[current_disease].add(med)

                    continue

                if current_disease and is_medicine_line(line):
                    med = re.sub(r"\s+", " ", line).strip()
                    data[current_disease].add(med)

    return data


def save_to_csv(data: dict, output_path: str) -> None:
    with open(output_path, "w", newline="", encoding="utf-8") as csvfile:
        writer = csv.writer(csvfile, quoting=csv.QUOTE_ALL)
        writer.writerow(["Disease", "Medicine"])
        for disease, meds in data.items():
            clean_disease = disease.replace(",", "") if disease else ""
            for med in sorted(meds):
                writer.writerow([clean_disease, med])


def main():
    parser = argparse.ArgumentParser(description="Extract disease -> medicine from CEAF PDF and save CSV")
    parser.add_argument("pdf", help="Path to input PDF")
    parser.add_argument("output", nargs="?", default="diseases_medicines.csv", help="Output CSV path")
    args = parser.parse_args()

    result = extract_disease_medicine(args.pdf)
    save_to_csv(result, args.output)
    print(f"CSV generated: {args.output}")


if __name__ == "__main__":
    main()
