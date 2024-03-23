# build_graph.py v0.3

import re
from helpers.loader import load_text

import logging
DEBUG_MODE = True
logger = logging.getLogger(__name__)

# import networkx as nx
# import matplotlib.pyplot as plt
# from networkx.drawing.nx_agraph import graphviz_layout

def init_logging():
    logging.basicConfig(filename='sqlgraph.log', level=logging.INFO)
    logger.info('START')

def log_info(message):
    if DEBUG_MODE == True:
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
    result=[]
    target_table = None
    log_info('Section to analyze:')
    log_info('-------------------')
    log_info(section_text)
    log_info('---------------------------------------------------')
    log_info('RESULT:')

    # Regular expression patterns
    target_table_pattern = r'^\w+(\.\w+)?'
    nested_section_pattern = r'\bWITH\b(.*?)\)\s*SELECT\b'

    # Find the target table using the pattern
    target_table_match = re.search(target_table_pattern, section_text)
    if target_table_match:
        target_table = target_table_match.group()

    # Check if there is a nested section (CTE)
    nested_sections = re.findall(nested_section_pattern, section_text, flags=re.DOTALL)

    updated_section_text = section_text  # Initialize the variable

    if nested_sections:
        for nested_section_content in nested_sections:
            nested_section_content = nested_section_content.strip()
            updated_section_text = updated_section_text.replace(nested_section_content, '')
            ctes = analyze_cte(nested_section_content + ')', module_name, action_type)
            if ctes:
                for cte in ctes:
                    result.append(cte)

        updated_section_text = re.sub(r'WITH\s*\)', '', updated_section_text)  # Remove 'WITH )'
    for r in result:
        log_info(r)

    # Find source tables
    source_table_pattern = r'(?:FROM|JOIN)\s+(\w+(?:\.\w+)?)'
    source_tables = re.findall(source_table_pattern, updated_section_text, flags=re.IGNORECASE)
    new_pair = {'target table': target_table, 'source tables': source_tables, 'module': module_name, 'edge_type': action_type}
    log_info(new_pair)
    result.append(new_pair)
    # 
    log_info('---------------------------------------------------')
    return result


# identifies the boundaries of common table expressions (CTE) and 
# calls the analyze_section() for each CTE
#  
def analyze_cte(cte_text, module_name='', action_type='insert'):
    result = []

    # Regular expression pattern to find section starts and ends
    section_pattern = r'(\w+)\s+AS\s+\('

    # Find all section starts and their positions
    section_starts = [(match.group(1), match.start()) for match in re.finditer(section_pattern, cte_text)]

    # Extract the content of each section and store table_name and content in a list of dictionaries
    cte_sections = []
    for i in range(len(section_starts)):
        table_name, start_pos = section_starts[i]

        # If it's the last section, end position is ';', otherwise it's before the next section start
        if i + 1 < len(section_starts):
            end_pos = section_starts[i + 1][1]
        else:
            end_pos = cte_text.find(';', start_pos)

        section_content = table_name+' AS ' + cte_text[start_pos + len(table_name) + 4: end_pos].strip()
        # cte_sections.append({'table_name': table_name, 'content': section_content})
        cte_sections.append( section_content)

    if cte_sections:
        for x in cte_sections:
            relationship = analyze_section(x, module_name, action_type)  # (recursive) call to analyze_section function
            if relationship:
                for r in relationship:
                    result.append(r)

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
        # graph = build_graph(analysis_results)
        # visualize_graph(graph)

if __name__ == '__main__':
    init_logging()    
    filename = input("Enter the file name (or press Enter for default): ").strip()
    if not filename:
        filename = 'samples/sample_4.sql'
    do_job(filename)
