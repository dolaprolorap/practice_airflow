from pathlib import Path
from logger import get_logger

import string
import os
import random
import logging


_FILE_EXCHANGE_FOLDER = os.path.join(os.path.dirname(__file__)) / Path('../file_exchange_data')


def save_file(
        file_name: str,
        content: str,
        file_exchange_folder: Path = _FILE_EXCHANGE_FOLDER,
        logger: logging.Logger = get_logger(__name__)
) -> str:
    hashed_file_name = _add_hash_to_file_name(file_name)
    full_path = file_exchange_folder / hashed_file_name

    with open(full_path, 'w') as file:
        file.write(content)

    logger.info(f'Файл {file_name} сохранен в {full_path}')

    return str(full_path)


def read_file(file_name: str) -> str:
    with open(file_name, 'r') as file:
        return file.read()


def delete_file(
        file_name: str,
        logger: logging.Logger = get_logger(__name__)
) -> None:
    os.remove(file_name)
    logger.info(f'Файл {file_name} удален')


def _add_hash_to_file_name(file_name: str, hash_len: int = 20) -> str:
    path = Path(file_name)

    return '{}-{}.{}'.format(
        path.stem,
        _generate_random_string(hash_len),
        path.suffix,
    )


def _generate_random_string(length: int) -> str:
    return ''.join(random.choices(string.ascii_uppercase + string.digits, k=length))
