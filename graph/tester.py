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


def run_test(file_name):
    file = open(file_name, 'r')
    test_content = yaml.safe_load(file)
    log_info(f'TEST: {file_name} \n ==============================================================')
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

            test_passed = (tr==ar)
            if test_passed:
                log_info('Result: PASSED') 
            else:
                log_info('Result: FAILED')
            log_info(f'    expected: {tr}')
            log_info(f'    received: {ar}')
    log_info(' ==============================================================')

    

if __name__ == '__main__':

    DEBUG_MODE = True   # Uncomment this line to write debug logs
    init_logging()
    fl = get_file_list(test_path)
    for f in fl:
        run_test(f)
