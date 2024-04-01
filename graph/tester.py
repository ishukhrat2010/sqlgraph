import os
from pathlib import Path
import yaml
import logging
from builder import analyze_text

test_path = 'tests'
DEBUG_MODE = False
logger = logging.getLogger(__name__)


def init_logging():
    if not DEBUG_MODE:
        return
    logging.basicConfig(filename='test_debug.log', level=logging.INFO)
    logger.info('START')


def log_info(message):
    if not DEBUG_MODE:
        return    
    logger.info(message)


def get_file_list(aPath):    
    if Path(aPath).is_absolute():
        cwd = aPath
    else:
        cwd = os.path.dirname(__file__)+'/'+aPath.strip('/')
    l=os.listdir(cwd)
    file_list = [f'{cwd}/{x}' for x in l if (os.path.isfile(f'{cwd}/{x}') and(Path(x).suffix=='.yaml' or Path(x).suffix=='.yml'))]

    return file_list


def compare_results(a,b):
    target_equal = (not a['target'] and not b['target']) or (a['target'] and b['target'] and a['target']==b['target'])
    if not target_equal:
        return False

    type_equal = (not a['edge_type'] and not b['edge_type']) or (a['edge_type'] and b['edge_type'] and a['edge_type']==b['edge_type'])
    if not type_equal:
        return False

    if not a['sources']:
        a_empty = True
    else:
        a_empty = (a['sources']==None or a['sources']==[] or a['sources']=='')
    if not b['sources']:
        b_empty = True
    else:
        b_empty = (b['sources']==None or b['sources']==[] or b['sources']=='')

    sources_equal = (a_empty and b_empty) or (not a_empty and not b_empty and a['sources']==b['sources'])
    return sources_equal


def run_test(file_name):
    file = open(file_name, 'r')
    test_content = yaml.safe_load(file)
    log_info(f' ============================================================== \n TEST: {file_name}')
    if test_content['description']:
        log_info('DESCRIPTION:  '+test_content['description'])
    if test_content['tests']:
        for t in test_content['tests']:
            q  = t['query_text']
            tr = t['result']
            ar = analyze_text(q)[0]
            if t['name']:
                test_name=t['name']
            else:
                test_name='test ***'
            log_info(test_name)

            test_passed = compare_results(tr, ar)
            if test_passed:
                log_info('Result: PASSED') 
            else:
                log_info('Result: FAILED')
            log_info(f'    expected: {tr}')
            log_info(f'    received: {ar}')
    log_info(' <<<<<<<<<<<<<<< \n')

    

if __name__ == '__main__':

    DEBUG_MODE = True   # Uncomment this line to write debug logs
    init_logging()
    fl = get_file_list(test_path)
    for f in fl:
        run_test(f)
