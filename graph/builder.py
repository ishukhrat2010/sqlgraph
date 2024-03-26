# builder.py v0.1

import re
from helpers.loader import load_text

import logging

# import networkx as nx
# import matplotlib.pyplot as plt
# from networkx.drawing.nx_agraph import graphviz_layout

DEBUG_MODE = False
logger = logging.getLogger(__name__)

def init_logging():
    if not DEBUG_MODE:
        return

    logging.basicConfig(filename='builder_debug.log', level=logging.INFO)
    logger.info('START')

def log_info(message):
    if not DEBUG_MODE:
        return
    
    logger.info(message)


# Removes commented lines or block commented with /*  */
def remove_commented_lines(text):
    
    pattern = r'\n|(--|/\*|\*/)'
    split_text = re.split(pattern, text, flags=re.M)

    filtered_lines = []
    singleline_comment = False
    multiline_comment = False

    for line in split_text:
        if (line==None) or (line.strip()==''):
            continue
        is_commented = (singleline_comment or multiline_comment)    # this section is inside the commented area
        
        if not is_commented:
            if line.strip().startswith('--'):    # start of the one-line comment, ignore next part
                singleline_comment = True
                continue
            elif line.strip().startswith('/*'):  # start of the multi-line comment, ignore next parts until '*/' is reached
                multiline_comment = True
                continue
            if line!='' and line.strip!='\n':
                filtered_lines.append(line)
        else:
            if singleline_comment:
                singleline_comment = False    # toggle back the singleline comment flag
                continue
            if (line.strip().startswith('*/') and multiline_comment):  # toggle back multiline comment flag
                multiline_comment = False
                continue        
    
    return '\n'.join(filtered_lines)


# Main function that identifies target and source tables in a sql-statement
# The section_text supposed to start with a target table name
def analyze_section(section_text, module_name='', action_type = 'insert'):

    def log_start(stext):
        log_info('-----------------------------------------------------------------')
        log_info('Section to analyze:')
        log_info(stext)
        log_info('-----------------------------------------------------------------')
        log_info('RESULT:')

    def log_result():
        pass

    def analyze_cte(atext):
        # Regular expression pattern to match CTEs
        pattern = r'WITH\s+([a-zA-Z0-9_]+)\s+AS\s+\((.*?)\)(?:\s*,\s*([a-zA-Z0-9_]+)\s+AS\s+\((.*?)\))*'
        match_span = (-1, -1)

        matches = re.search(pattern, atext, re.IGNORECASE | re.DOTALL)

        if matches:
            match_span = matches.span()
            cte_pairs = matches.groups()
            cte_names = cte_pairs[::2]
            cte_definitions = cte_pairs[1::2]
            for cte_name, cte_definition in zip(cte_names, cte_definitions):
                if (cte_name==None) or (cte_definition==None):
                    continue
                sub_section = cte_name + ' ' + cte_definition.strip()
                # recursive call
                cte_result=analyze_section(sub_section, module_name, 'insert')  # consider changing 'insert' to 'cte'
                result.append(cte_result)
            # section_text = re.sub(pattern, '', section_text)
        return match_span


    result=[]
    target_table = None

    # Regular expression patterns
    target_table_pattern = r'^\w+(\.\w+)?'
    # nested_section_pattern = r'\bWITH\b(.*?)\)\s*SELECT\b'

    # Find the target table using the pattern
    target_table_match = re.search(target_table_pattern, section_text)
    if target_table_match:
        target_table = target_table_match.group()

    start, end = analyze_cte(section_text)

    # # Check if there is a nested section (CTE)
    # nested_sections = re.findall(nested_section_pattern, section_text, flags=re.DOTALL)

    if (start >=0) and (end>=0):
        updated_section_text = section_text[:start] + section_text[end:]
    else:
        updated_section_text = section_text
        log_start(updated_section_text)

    # Find source tables
    source_table_pattern = r'(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)'
    source_tables = re.findall(source_table_pattern, updated_section_text, flags=re.IGNORECASE)
    new_pair = {'target table': target_table, 'source tables': source_tables, 'module': module_name, 'edge_type': action_type}
    log_info(new_pair)
    log_info('-----------------------------------------------------------------')
    result.append(new_pair)
    return result



# Analyzes SQL script and returns identified graph nodes 
def analyze_text(text, module_name=''):
    # Regular expression patterns
    section_start_pattern = r'\b(insert(?:\s+into)|create table|create temp table|create temporary table|update)\b'
    rex_obj = re.compile(section_start_pattern, flags=re.IGNORECASE)
    section_end_pattern = r';'

    # Let's find all spots where table is created or records are inserted
    # sections = re.split(section_start_pattern, text, flags=re.IGNORECASE)
    sections = rex_obj.split(text)
    results = []

    for i in range(1, len(sections), 2):
        section_start = sections[i]
        if section_start.lower() == 'update':
            action='update'
        else:
            action='insert'

        section_content = sections[i + 1].strip()

        # Find the position of the section's end (semicolon)
        semicolon_match = re.search(section_end_pattern, section_content)
        if semicolon_match:
            section_end = semicolon_match.start() + 1
        else:
            # If no semicolon found, consider the entire section content as the section
            section_end = len(section_content)

        # Extract the section from the content
        section = section_content[:section_end].strip()

        analysis_result = analyze_section(section, module_name, action)
        if analysis_result:
            for s in analysis_result:
                results.insert(0, s)

    return results


# def build_graph(analysis_results):
#     graph = nx.DiGraph()

#     for result in analysis_results:
#         target_table = result['target table']
#         source_tables = result['source tables']

#         for source_table in source_tables:
#             graph.add_edge(source_table, target_table)
#     return graph

# def visualize_graph(graph):
#     # Visualization as hierarchy with circular nodes (optional)
#     pos = graphviz_layout(graph, prog='dot', args='-Grankdir=BT')  # Set rankdir to top to bottom (TB)
#     plt.figure(figsize=(50, 20))
#     node_labels = {node: node.split('.')[-1] for node in graph.nodes()}
#     node_sizes = [len(label) * 1200 for label in node_labels.values()]  # Adjusted node size
#     nx.draw_networkx(graph, pos, with_labels=True, labels=node_labels, node_shape='o', node_size=node_sizes, font_size=10, font_weight='bold', arrowsize=20)
#     plt.show()


def do_job(fname):
    file_content = load_text(fname)
    if file_content is not None:
        text_without_comments = remove_commented_lines(file_content)        
        analysis_results = analyze_text(text_without_comments, fname)
        print(analysis_results)
        # graph = build_graph(analysis_results)
        # visualize_graph(graph)

if __name__ == '__main__':

    # DEBUG_MODE = True   # Uncomment this line to write debug logs
    init_logging()
    
    filename = input("Enter the file name (or press Enter for default): ").strip()
    if not filename:
        filename = 'samples/sample_5.sql'
    do_job(filename)
