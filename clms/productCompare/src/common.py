import os

def compare_file_info(file1, file2):
    file_info = {
        'filename': (file1, file2),
        'filesize': (os.path.getsize(file1), os.path.getsize(file2))
        }
    return file_info
