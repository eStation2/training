import zipfile
from common import compare_file_info

def compare_zip_contents(file1, file2):
    with zipfile.ZipFile(file1, 'r') as zf:
        contents1 = zf.namelist()
    print(contents1)
    with zipfile.ZipFile(file2, 'r') as zf:
        contents2 = zf.namelist()
    print(contents2)

    # Compare contents
    diffs = {
        'only_in_ref': list(set(contents1) - set(contents2)),
        'only in_new': list(set(contents2) - set(contents1))
    }

    return diffs

def compare_zip(file1, file2):
    report = {}
    print("File info")
    report['file_info'] = compare_file_info(file1, file2)
    
    print("zip_diff")
    report['zip_diff'] = compare_zip_contents(file1, file2)
   
    return report
  
if __name__ == "__main__":
    import argparse
    import json
    from pprint import pprint
    
    parser = argparse.ArgumentParser(description="Compare two ZIP files")
    parser.add_argument("ref_zip", type=str, help="Reference ZIP file")
    parser.add_argument("new_zip", type=str, help="New ZIP file")
    parser.add_argument("-r", "--reportFile", type=str, required=False, default=None, help="Report file - json file containing differences")
    parser.add_argument('-q', '--quiet', action='store_true', help='Suppress terminal output')
    
    args = parser.parse_args()  
    report = compare_zip(args.ref_zip, args.new_zip)

    if not args.quiet:
        pprint(report, sort_dicts=False)

    if args.reportFile:
        print(f"Saving report to {args.reportFile}")
        with open(args.reportFile, 'w') as fp:
            fp.write(json.dumps(report, indent=2))
