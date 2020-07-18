import os
import sys
import json
import requests
import logging as log
import traceback
import time
import string
import argparse as arg
from typing import List, Tuple, Dict
from lxml import etree
from fake_useragent import UserAgent
from bs4 import BeautifulSoup

IMG_BASE = 'https://www.ncbi.nlm.nih.gov'
USE_PROXY = False
OUTPUT_DIR = 'reader_info/'
PROXY_POOL_BASE = 'http://118.24.52.95'
PMID_SOURCE = ''
LOCKFILE = 'pubmed_info.reader.lock'
FAILEDFILE = 'failed.json'
REQUESTS_PARAM = {
    'timeout': 30
}

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
        fetch_count = 10
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


def get_pmc_reader_html(pmid):
    url = f'https://www.ncbi.nlm.nih.gov/pmc/articles/pmid/{pmid}/?report=reader'
    response = get_html(url, use_proxy=USE_PROXY)
    if not response or response.status_code != requests.codes['\\o/']:
        log.warning("Failed to retrieve data from sever for pmid %d.", pmid)
        log.warning("This might be a temporary problem. Use argument --retry for a retry.")
        return None
    return response.content


def dowload_figure(pmid, html):
    figs = []
    soup = BeautifulSoup(html, 'html.parser')
    el_figs = soup.select('.fig.iconblock')
    for el_fig in el_figs:
        # get id
        el_anchor = el_fig.find('a', recursive=False)
        if el_anchor is None or not el_anchor.has_attr('rid-figpopup'):
            continue
        id = el_anchor['rid-figpopup']
        # get image source
        el_img = el_fig.find('img')
        if el_img.has_attr('src-large'):
            src = el_img['src-large']
        else:
            src = el_img['src']
        # get desc
        el_desc = el_fig.find(class_='icnblk_cntnt')
        el_name = el_desc.findChild('div')
        name = el_name.get_text()
        el_name.extract()
        caption = el_desc.get_text().replace('\n', ' ')
        # download image
        url = IMG_BASE + src
        path = 'images/'
        filename = f"{pmid}_{id}." + src[-3:]
        download_to(url, pmid, filename, path)
        # save
        figs.append({
            'id': id,
            'name': name,
            'caption': caption,
            'src': src,
            'filepath': path + filename
        })
    return figs


def deal_with_para(el_para):
    if el_para is None:
        return None
    para_id = el_para['id'] if el_para.has_attr('id') else '<unk>'
    # For figures, use short code schema
    figs = []
    el_figs = el_para.find_all(class_='figpopup')
    for el_fig in el_figs:
        if not el_fig.has_attr('rid-figpopup'):
            continue
        fig_id = el_fig['rid-figpopup']
        fig_text = el_fig.get_text()
        el_fig.replace_with(f"[figref id=\"{fig_id}\"]{fig_text}[/figref]")
        figs.append(fig_id)
    return {
        'id': para_id,
        'content': el_para.get_text().replace('\n', ' '),
        'figs': figs
    }

def parse_content(html):
    soup = BeautifulSoup(html, 'html.parser')
    el_title = soup.find(class_="content-title")
    title = el_title.get_text() if el_title is not None else "<unk>"
    title = title.replace('\n', ' ')
    el_author = soup.find(class_="contribs")
    author = el_author.get_text() if el_author is not None else "<unk>"
    author = author.replace('\n', ' ')
    # For secs
    secs = []
    el_secs = soup.find_all(class_="tsec")
    for el_sec in el_secs:
        [el.extract() for el in el_sec.find_all('sup')] # No refs
        sec_id = el_sec['id']
        if sec_id == '__ffn_sec':
            # Ignore 'Article information'
            continue
        # Head
        el_head = el_sec.find(class_="head")
        head = el_head.get_text() if el_head is not None else "<unk>"
        head = head.replace('\n', ' ')
        if head.lower() == 'references':
            continue
        # For paras
        paras = []
        el_paras = el_sec.find_all('p', recursive=False)
        for el_para in el_paras:
            para_data = deal_with_para(el_para)
            if para_data is not None:
                paras.append(para_data)
        # For subsec
        sub_sec = []
        el_sub_secs = el_sec.find_all(class_="sec", recursive=False)
        for el_sub_sec in el_sub_secs:
            sec_id = el_sub_sec['id'] if el_sub_sec.has_attr('id') else "<unk>"
            # Keyword secs
            if el_sub_sec.find(class_='kwd-title') is not None:
                sub_sec.append({
                    'id': sec_id,
                    'head': 'Keywords',
                    'paras': [{
                        'id': '<unk>',
                        'content': el_sub_sec.find(class_='kwd-text').get_text()
                    }]
                })
                continue
            # Not keyword
            el_head = el_sub_sec.find('h3')
            sub_head = el_head.get_text() if el_head is not None else "<unk>"
            sub_head = sub_head.replace('\n', ' ')
            # For paras
            sub_paras = []
            el_paras = el_sub_sec.find_all('p', recursive=False)
            for el_para in el_paras:
                para_data = deal_with_para(el_para)
                if para_data is not None:
                    sub_paras.append(para_data)
            sub_sec.append({
                'id': sec_id,
                'head': sub_head,
                'paras': sub_paras
            })
        # Result
        secs.append({
            'id': sec_id,
            'head': head,
            'paras': paras,
            'sub_secs': sub_sec
        })

    return {
        'title': title,
        'author': author,
        'section': secs
    }


def download_info(pmid):
    # Search for figure
    html = get_pmc_reader_html(pmid)
    try:
        imgs = dowload_figure(pmid, html)
        data = parse_content(html)
        data['images'] = imgs
    except Exception as e:
        log.warning("Error in downloading info for pmid %d", pmid)
        log.warning("%s\n%s", e, traceback.format_exc())
        return False
    # Save
    try:
        if not os.path.exists(OUTPUT_DIR):
            os.mkdir(OUTPUT_DIR)
        path = os.path.join(OUTPUT_DIR, 'content/')
        if not os.path.exists(path):
            os.mkdir(path)
        filename = f"{path}{pmid}.json"
        with open(filename, 'w') as f:
            json.dump(data, f)
    except Exception as e:
        log.error(f"Unable to write result for pmid %d! %s", pmid, e)
        return False
    return True


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


def parse_arguments():
    parser = arg.ArgumentParser(
        description='Download info from pubmed central by PMIDs')
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
        if download_info(source[idx]):
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
