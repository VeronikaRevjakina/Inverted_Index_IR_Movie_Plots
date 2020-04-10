import os
import shutil

import psutil

from constants import BUILDED_INDEX_PATH, FINAL_INDEX_PATH

process = psutil.Process(os.getpid())


def create_directories():
    """
    Create packages for data storing and clean files if exist.
    """
    for folder in [BUILDED_INDEX_PATH, FINAL_INDEX_PATH]:
        if not os.path.isdir(folder):
            os.makedirs(folder)
        else:
            for filename in os.listdir(folder):
                file_path = os.path.join(folder, filename)
                try:
                    if os.path.isfile(file_path) or os.path.islink(file_path):
                        os.unlink(file_path)
                    elif os.path.isdir(file_path):
                        shutil.rmtree(file_path)
                except Exception as e:
                    print('Failed to delete %s. Reason: %s' % (file_path, e))
