import os
import sys
import json
import requests
import logging as log
import traceback
from typing import List, Tuple
import argparse as arg
from lxml import etree
from fake_useragent import UserAgent

PDF_BASE = 'https://www.ncbi.nlm.nih.gov/'
USE_PROXY = False
OUTPUT_DIR = 'pmc_pdfs/'
PROXY_POOL_BASE = 'http://118.24.52.95'
PMID_SOURCE = ''
LOCKFILE = 'pubmed_central.lock'
FAILEDFILE = 'failed.json'
REQUESTS_PARAM = {
    'timeout': 30
}

log.basicConfig(level=log.INFO,
                format='%(asctime)s:%(lineno)d - %(levelname)s: %(message)s')

ua = UserAgent()
USER_AGENT = ua.random


def get_proxy():
    return requests.get(f"{PROXY_POOL_BASE}/get/").json()


def delete_proxy(proxy):
    requests.get("{}/get/delete/?proxy={}".format(PROXY_POOL_BASE, proxy))


def get_html(url, use_proxy=USE_PROXY):
    """
    Get html from url
    """
    retry_count = 5
    # Proxy config
    if use_proxy:
        proxy = get_proxy().get("proxy")
        proxies = {"http": "http://{}".format(proxy)}
    else:
        proxies = None
    # Headers
    headers = {
        'User-Agent': USER_AGENT
    }
    # Start
    while retry_count > 0:
        try:
            html = requests.get(url, proxies=proxies, headers=headers, **REQUESTS_PARAM)
            return html
        except Exception:
            retry_count -= 1
    # Delete proxy
    if use_proxy:
        delete_proxy(proxy)
    log.warning("Fail to get url: %s, maximum retries count exceed.", url)
    return None


def download(file_path, url, headers=None, proxies=None):
    # Check file size
    r1 = requests.get(url, stream=True, headers=headers, proxies=proxies, **REQUESTS_PARAM)
    total_size = int(r1.headers['Content-Length'])
    if os.path.exists(file_path):
        temp_size = os.path.getsize(file_path)  # already downloaded
    else:
        temp_size = 0
    if temp_size >= total_size:
        return
    # Continue download
    headers['Range'] = 'bytes=%d-' % temp_size
    r = requests.get(url, stream=True, headers=headers, proxies=proxies, **REQUESTS_PARAM)
    with open(file_path, "ab") as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                temp_size += len(chunk)
                f.write(chunk)
                f.flush()
                done = int(50 * temp_size / total_size)
                sys.stdout.write("\r[%s%s] %.2f%%" % (
                    '=' * done, ' ' * (50 - done), 100 * temp_size / total_size))
                sys.stdout.flush()
    print()


def download_to(url, pmid, use_proxy=USE_PROXY):
    # Filename
    filename = f'{OUTPUT_DIR}{pmid}.pdf'
    # Proxy config
    if use_proxy:
        proxy = get_proxy().get("proxy")
        proxies = {"http": "http://{}".format(proxy)}
    else:
        proxies = None
    # Headers
    headers = {
        'User-Agent': USER_AGENT
    }
    # Download
    retry_count = 5
    err = None
    while retry_count > 0:
        try:
            download(filename, url, headers=headers, proxies=proxies)
            return True
        except Exception as e:
            err = e
            retry_count -= 1
    if use_proxy:
        delete_proxy(proxy)
    log.warning("Fail to download pdf: %s, maximum retries count exceed.", url)
    log.warning("%s\n%s", err, traceback.format_exc())
    return False


def get_pmc_html(pmid):
    url = f'https://www.ncbi.nlm.nih.gov/pmc/articles/pmid/{pmid}/'
    return get_html(url)


def download_pmc(pmid):
    log.info('Start download pdf for pmid %d', pmid)
    response = get_pmc_html(pmid)
    html = etree.HTML(response.content)
    pdf_tag = html.xpath('//td[@class="format-menu"]//a[contains(@href,".pdf")]'
                         + '|//div[@class="format-menu"]//a[contains(@href,".pdf")]'
                         + '|//aside[@id="jr-alt-p"]/div/a[contains(@href,".pdf")]')
    if len(pdf_tag) < 1:
        log.warning("No pdf found for pmid %d", pmid)
        return
    # Download
    pdf_url = pdf_tag[0].attrib['href']
    if pdf_url[0] == '/':
        pdf_url = PDF_BASE + pdf_url
    log.debug('Successful get pdf url (%s) pmid %d', pdf_url, pmid)

    try:
        if not os.path.exists(OUTPUT_DIR):
            os.mkdir(OUTPUT_DIR)
        result = download_to(pdf_url, pmid)
        log.info('Successful download pdf for pmid %d', pmid)
        return result
    except Exception as e:
        log.warning("Error in downloading %s for pmid %d", pdf_url, pmid)
        log.warning("%s\n%s", e, traceback.format_exc())
        return False


def parse_arguments():
    parser = arg.ArgumentParser(
        description='Download PDFs from pubmed central by PMIDs')
    parser.add_argument(dest='source', metavar='PMIDs or PMID source file',
                        nargs='*', help='PMIDs to download, or filepath of PMID source file.')
    parser.add_argument('-o', '--output-dir', dest='output_dir', action='store',
                        help='output directory')
    parser.add_argument('--resume', dest='resume', action='store_true',
                        help='Allow resume from an exist lock file')
    parser.add_argument('--retry', dest='retry', action='store_true',
                        help='Retry the tasks in the failed file')
    parser.add_argument('--use-proxy', dest='use_proxy', action='store_true',
                        help='Use proxy pool to access Pubmed Central')
    # Parse
    args = parser.parse_args()

    global USE_PROXY
    USE_PROXY = args.use_proxy

    if args.output_dir:
        global OUTPUT_DIR
        OUTPUT_DIR = args.output_dir
    return args


def load_source_file() -> List[int]:
    """
    Load pmid source from a source file
    """
    # Read
    try:
        with open(PMID_SOURCE, 'r') as f:
            data = json.load(f)
    except Exception as e:
        log.error("Unable to load source file %s! %s", PMID_SOURCE, e)
        quit()
    # Parse
    try:
        return [x['pmid'] for x in data]
    except Exception:
        log.error("Data source format invalid!")
        quit()


def load_source(args) -> List[int]:
    """
    Load pmid source
    """
    global PMID_SOURCE

    # Retry from failed file
    if args.retry:
        PMID_SOURCE = FAILEDFILE
        return load_source_file()

    if not args.source:
        log.error("No PMIDs or source file given!")
        quit()

    try:
        source = [int(x) for x in args.source]
        PMID_SOURCE = str(source)
        return source
    except Exception:
        # Parse as filepath
        PMID_SOURCE = args.source[0]
        return load_source_file()


def resume_from_lock(source: List[int], resume=False) -> Tuple[int, List[int]]:
    # Check lock
    if os.path.exists(LOCKFILE):
        if not resume:
            log.error("Lock file exists! There might be a running task!")
            log.error("Use arguments --resume to resume from previous work.")
            quit()
        # Resume from lock
        log.info("Lock file exists, try to resume from previous work...")
        try:
            with open(LOCKFILE, 'r') as f:
                lock = json.load(f)
            if lock['source'] != PMID_SOURCE:
                raise Exception()
            if lock['length'] != len(source):
                raise Exception()
            failed = []
            if 'failed' in lock and isinstance(lock['failed'], list):
                failed = [int(x) for x in lock['failed']]
            return int(lock['progress']), failed
        except Exception:
            log.info("Invalid lock, probably from an old task, ignored.")
    # Create lock
    update_lock(source, 0)
    log.info("Create lock file %s", LOCKFILE)
    return 0, []


def update_lock(source, progress=0, failed=[]):
    lock = {
        'source': PMID_SOURCE,
        'length': len(source),
        'progress': progress,
        'failed': failed
    }
    try:
        with open(LOCKFILE, 'w') as f:
            json.dump(lock, f)
    except Exception as e:
        log.error("Unable to write lock file! %s", e)
        quit()


def clear_lock():
    if os.path.exists(LOCKFILE):
        os.unlink(LOCKFILE)


def save_failed(failed):
    data = [{'pmid': x} for x in failed]
    try:
        with open(FAILEDFILE, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        log.error("Unable to write failed file! %s", e)
        quit()
    log.warning("Save failed file to %s!", FAILEDFILE)
    log.warning("Using --retry to retry the tasks in the failed file.")


if __name__ == "__main__":
    args = parse_arguments()
    # Load PMID soruce
    source = load_source(args)
    # Start downloading
    total = len(source)
    start_at, failed = resume_from_lock(source, resume=args.resume)
    for idx in range(start_at, total):
        update_lock(source, idx, failed)
        if download_pmc(source[idx]):
            pass
        else:
            failed.append(source[idx])
    # Finish
    failed_count = len(failed)
    log.info('Completely download %d PDFs, failed %d',
             total - failed_count, failed_count)
    if failed_count > 0:
        log.warning('Failed to fetch PMIDs: %s%s',
                    ', '.join(map(str, failed[:5])),
                    ' and more...' if failed_count > 5 else '')
        save_failed(failed)
    clear_lock()
