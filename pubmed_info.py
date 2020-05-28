import os
import sys
import json
import requests
import logging as log
import traceback
import time
from typing import List, Tuple, Dict
import argparse as arg
from lxml import etree
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

PDF_BASE = 'https://www.ncbi.nlm.nih.gov/'
USE_PROXY = False
OUTPUT_DIR = 'info/'
PROXY_POOL_BASE = 'http://118.24.52.95'
PMID_SOURCE = ''
LOCKFILE = 'pubmed_info.lock'
FAILEDFILE = 'failed.json'
REQUESTS_PARAM = {
    'timeout': 30
}

OPTION_BASIC = False
OPTION_PIC = True
OPTION_PDF = False

log.basicConfig(level=log.INFO,
                format='%(asctime)s:%(lineno)d - %(levelname)s: %(message)s')

ua = UserAgent()
USER_AGENT = ua.random

cur_proxy = None
fetch_count = 0

def get_proxy(refresh=False):
    global cur_proxy
    global fetch_count
    if cur_proxy is None or refresh or fetch_count > 10:
        try:
            cur_proxy = requests.get(f"{PROXY_POOL_BASE}/get/").json().get('proxy')
            log.info("Renew proxy %s", cur_proxy)
        except Exception:      
            time.sleep(30)
            get_proxy(refresh=True)
    fetch_count += 1
    return cur_proxy


def delete_proxy():
    requests.get('{}/get/delete/?proxy={}'.format(PROXY_POOL_BASE, cur_proxy))


def get_html(url, use_proxy=USE_PROXY):
    """
    Get html from url
    """
    retry_count = 5
    # Proxy config
    if use_proxy:
        proxy = get_proxy()
        proxies = {'http': 'http://{}'.format(proxy)}
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
        except Exception as e:
            retry_count -= 1
            log.debug("Probleam in fetching url %s: %s", url, e)
            # Refresh proxy
            if use_proxy:
                proxy = get_proxy(refresh=True)
                proxies = {'http': 'http://{}'.format(proxy)}
    # Delete proxy
    if use_proxy:
        delete_proxy()
    log.warning("Fail to get url: %s, maximum retries count exceed.", url)
    return ''


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
    with open(file_path, 'ab') as f:
        for chunk in r.iter_content(chunk_size=1024):
            if chunk:
                temp_size += len(chunk)
                f.write(chunk)
                f.flush()
                done = int(50 * temp_size / total_size)
                sys.stdout.write('\r[%s%s] %.2f%%' % (
                    '=' * done, ' ' * (50 - done), 100 * temp_size / total_size))
                sys.stdout.flush()
    print()


def download_to(url, pmid, filename, path='./', use_proxy=USE_PROXY):
    if not os.path.exists(OUTPUT_DIR):
        os.mkdir(OUTPUT_DIR)
    path = os.path.join(OUTPUT_DIR, path)
    if not os.path.exists(path):
        os.mkdir(path)
    filename = path + filename
    # Proxy config
    if use_proxy:
        proxy = get_proxy()
        proxies = {'http': 'http://{}'.format(proxy)}
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
            return filename
        except Exception as e:
            err = e
            retry_count -= 1
            # Refresh proxy
            if use_proxy:
                proxy = get_proxy(refresh=True)
                proxies = {'http': 'http://{}'.format(proxy)}
    if use_proxy:
        delete_proxy()
    log.warning("Fail to download file: %s, maximum retries count exceed.", url)
    log.warning("%s\n%s", err, traceback.format_exc())
    return False


def get_pubmed_html(pmid):
    url = f'https://pubmed.ncbi.nlm.nih.gov/{pmid}/'
    response = get_html(url, use_proxy=USE_PROXY)
    if not response or response.status_code != requests.codes['\\o/']:
        log.warning("Failed to retrieve data from sever for pmid %d.", pmid)
        log.warning("This might be a temporary problem. Use argument --retry for a retry.")
        return None
    return response.content

def download_basic_info(pmid):
    # TODO: use pymed
    return True

FIGURE_RESULT = []

def download_figure(pmid):
    pmid = source[idx]['pmid']
    pubmed_html = get_pubmed_html(pmid)
    if pubmed_html is None:
        return False
    
    # Search for figure
    ret = []
    try:
        # Get figures-list
        soup = BeautifulSoup(pubmed_html, 'html.parser')
        figures_list = soup.find(class_='figures-list')
        if not figures_list:
            log.info("No figures for pmid %d", pmid)
            return True

        figures = figures_list.find_all('figure')
        for fig in figures:
            img_id = fig['data-label-slug']
            img_url = fig.find(class_='figure-link')['href']
            caption = fig.find('figcaption').find(class_='figure-caption-contents').get_text()
            dest_filename = f'{pmid}_{img_id}' + img_url[-4:]
            dest = download_to(img_url, pmid, dest_filename, path='images/', use_proxy=USE_PROXY)
            if not dest:
                log.warning("Error in downloading figures %s for pmid %d", img_id, pmid)
                return False
            ret.append({
                'id': img_id,
                'url': img_url,
                'caption': caption,
                'local_path': dest
            })
    except Exception as e:
        log.warning("Error in downloading figures for pmid %d", pmid)
        log.warning("%s\n%s", e, traceback.format_exc())
        return False
    # Save
    FIGURE_RESULT.append({
        'pmid': pmid,
        'figures': ret
    })
    graph_data = os.path.join(OUTPUT_DIR, 'graph.json')
    try:
        with open(graph_data, 'w') as f:
            json.dump(FIGURE_RESULT, f)
    except Exception as e:
        log.error("Unable to write figure result! %s", e)
        quit()
    return True


def extract_text(pdf_path):
    # TODO
    return True


def parse_arguments():
    parser = arg.ArgumentParser(
        description='Download PDFs from pubmed central by PMIDs')
    parser.add_argument(dest='source', action='store', metavar='PDFs path',
                        help='PDFs path, PDF named as "PMID.pdf"')
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


def load_source_file() -> List[Dict]:
    """
    Load pmid source from a source file
    """
    # Read
    try:
        with open(PMID_SOURCE, 'r') as f:
            data = json.load(f)
        return data
    except Exception as e:
        log.error("Unable to load source file %s! %s", PMID_SOURCE, e)
        quit()


def load_source_dir() -> List[Dict]:
    ret = []
    for root, _, files in os.walk(PMID_SOURCE):
        for filename in files:
            path = os.path.join(root, filename)

            if filename[-4:].lower() != '.pdf':
                continue

            try:
                pmid = int(filename[:-4])
                ret.append({
                    'pmid': pmid,
                    'path': path
                })
            except Exception:
                log.warning("Error in loading source dir at file %s", path)
                continue
    return ret


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

    # Load graph cache
    graph_data = os.path.join(OUTPUT_DIR, 'graph.json')
    if os.path.exists(graph_data):
        global FIGURE_RESULT
        try:
            with open(graph_data, 'r') as f:
                FIGURE_RESULT = json.load(f)
        except Exception:
            FIGURE_RESULT = []

    PMID_SOURCE = args.source
    return load_source_dir()


def resume_from_lock(source: List[Dict], resume=False) -> Tuple[int, List[Dict]]:
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
                failed = lock['failed']
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
    try:
        with open(FAILEDFILE, 'w') as f:
            json.dump(failed, f)
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
        fail = False
        pmid = source[idx]['pmid']

        if OPTION_BASIC:
            if not download_basic_info(pmid):
                fail = True
        
        if OPTION_PIC:
            if not download_figure(pmid):
                fail = True

        if OPTION_PDF:
            if not extract_text(source[idx]['path']):
                fail = True

        if fail:
            failed.append(source[idx])
    # Finish
    failed_count = len(failed)
    log.info('Completely download %d PDFs, failed %d',
             total - failed_count, failed_count)
    if failed_count > 0:
        log.warning('Failed to fetch PMIDs: %s%s',
                    ', '.join(map(lambda x: str(x['pmid']), failed[:5])),
                    ' and more...' if failed_count > 5 else '')
        save_failed(failed)
    clear_lock()
