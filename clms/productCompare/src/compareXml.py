import xml.etree.ElementTree as ET

from common import compare_file_info


def compare_xml(file1, file2):
    report = {
        'file_info': compare_file_info(file1, file2)
    }

    tree1 = ET.parse(file1)
    tree2 = ET.parse(file2)

    root1 = tree1.getroot()
    root2 = tree2.getroot()

    diffs = []
    
    def recursive_compare(elem1, elem2, path=""):
        if elem1.tag != elem2.tag:
            diffs.append(f"Different tags at {path}: {elem1.tag} vs {elem2.tag}")
        if elem1.text == None:
            print (elem1.tag)
        if elem2.text == None:
            print (elem2.tag)
        if elem1.text and elem1.text.strip() != elem2.text.strip():
            diffs.append(f"Text mismatch at {path}/{elem1.tag}: {elem1.text} vs {elem2.text}")
        if elem1.attrib != elem2.attrib:
            diffs.append(f"Attributes mismatch at {path}/{elem1.tag}: {elem1.attrib} vs {elem2.attrib}")

        for sub_elem1, sub_elem2 in zip(elem1, elem2):
            recursive_compare(sub_elem1, sub_elem2, path + "/" + elem1.tag)

    recursive_compare(root1, root2)
    
    report['diffs'] = diffs
    return report

if __name__ == "__main__":
    import argparse
    import json
    from pprint import pprint
    
    parser = argparse.ArgumentParser(description="Compare two xml files")
    parser.add_argument("ref_xml", type=str, help="Reference xml file")
    parser.add_argument("new_xml", type=str, help="New xml file")
    parser.add_argument("-r", "--reportFile", type=str, required=False, default=None, help="Report file - json file containing differences")
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress terminal output')
    
    args = parser.parse_args()  
    report = compare_xml(args.ref_xml, args.new_xml)

    if not args.quiet:
        pprint(report, sort_dicts=False)

    if args.reportFile:
        print(f"Saving report to {args.reportFile}")
        with open(args.reportFile, 'w') as fp:
            fp.write(json.dumps(report, indent=2))
