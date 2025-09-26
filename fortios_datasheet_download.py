import requests
from bs4 import BeautifulSoup
from urllib.parse import urljoin, urldefrag
from pathlib import Path
from concurrent.futures import ThreadPoolExecutor, as_completed

REQUEST_TIMEOUT = (5, 20)
DOWNLOAD_TIMEOUT = (5, 60)
RESOLVE_WORKERS = 8
DOWNLOAD_WORKERS = 4


def extract_urls(page_url: str) -> set[str]:
    response = requests.get(page_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")
    urls: set[str] = set()

    for tag in soup.find_all("a", href=True):
        href = tag["href"].strip()
        if not href:
            continue

        absolute_url = urljoin(page_url, href)
        absolute_url, _ = urldefrag(absolute_url)
        urls.add(absolute_url)

    return urls


def clean_pdf_url(url: str) -> str | None:
    pdf_pos = url.lower().find(".pdf")
    if pdf_pos == -1:
        return None
    return url[: pdf_pos + 4]


def find_pdf_url(page_url: str) -> str | None:
    response = requests.get(page_url, timeout=REQUEST_TIMEOUT)
    response.raise_for_status()
    soup = BeautifulSoup(response.text, "html.parser")

    for tag in soup.find_all("a", href=True):
        candidate = tag["href"].strip()
        if not candidate:
            continue
        absolute_url = urljoin(page_url, candidate)
        cleaned = clean_pdf_url(absolute_url)
        if cleaned:
            return cleaned
    return None


def resolve_pdf_link(datasheet_url: str) -> tuple[str, str | None]:
    pdf_url = clean_pdf_url(datasheet_url)
    if pdf_url:
        return datasheet_url, pdf_url
    return datasheet_url, find_pdf_url(datasheet_url)


def download_pdf(url: str, output_dir: Path) -> Path:
    cleaned_url = clean_pdf_url(url)
    if not cleaned_url:
        raise ValueError(f"No PDF detected in URL: {url}")
    filename = cleaned_url.rsplit("/", 1)[-1] or "datasheet.pdf"
    filepath = output_dir / filename
    if filepath.exists():
        return filepath
    response = requests.get(cleaned_url, timeout=DOWNLOAD_TIMEOUT)
    response.raise_for_status()
    filepath.write_bytes(response.content)
    return filepath


def main() -> None:
    page_url = "https://www.fortinet.com/products/next-generation-firewall"
    datasheet_urls = sorted(
        url for url in extract_urls(page_url) if "data-sheets" in url.lower()
    )
    output_dir = Path("resources") / "datasheet_downloads"
    output_dir.mkdir(parents=True, exist_ok=True)

    resolved: list[tuple[str, str]] = []
    with ThreadPoolExecutor(max_workers=RESOLVE_WORKERS) as executor:
        futures = {executor.submit(resolve_pdf_link, url): url for url in datasheet_urls}
        for future in as_completed(futures):
            source_url, pdf_url = future.result()
            if pdf_url:
                resolved.append((source_url, pdf_url))
            else:
                print(f"No PDF link found for {source_url}")

    seen: set[str] = set()
    download_jobs: list[tuple[str, str]] = []
    for source_url, pdf_url in resolved:
        if pdf_url not in seen:
            seen.add(pdf_url)
            download_jobs.append((source_url, pdf_url))
        else:
            print(f"{source_url} -> {pdf_url} (already queued)")

    with ThreadPoolExecutor(max_workers=DOWNLOAD_WORKERS) as executor:
        future_to_pdf = {
            executor.submit(download_pdf, pdf_url, output_dir): (source_url, pdf_url)
            for source_url, pdf_url in download_jobs
        }
        for future in as_completed(future_to_pdf):
            source_url, pdf_url = future_to_pdf[future]
            try:
                filepath = future.result()
                print(f"{source_url} -> {pdf_url} saved to {filepath.name}")
            except Exception as exc:
                print(f"{source_url} -> {pdf_url} failed: {exc}")


if __name__ == "__main__":
    main()