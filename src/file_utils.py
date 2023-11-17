
from pathlib import Path

def get_file_without_extension(file:str):
    return file.rsplit('.', 1)[0]

def remove_files(files_to_remove):
    for path in files_to_remove:
        p = Path(path)
        p.unlink(missing_ok=True)



        
