import logging
import os
from concurrent.futures import ThreadPoolExecutor
from zipfile import ZipFile

from google.cloud import storage


def download_files_from_public_gs_bucket(
        bucket_name: str,
        destination_folder: str,
        max_concurrent=5,
        max_files: (int, None) = None
):
    if not os.path.exists(destination_folder):
        raise ValueError("Destination folder doesn't exist.")

    client = storage.Client.create_anonymous_client()
    bucket = client.bucket(bucket_name)

    downloaded = list()
    with ThreadPoolExecutor(max_workers=max_concurrent) as executor:
        for i, blob in enumerate(bucket.list_blobs()):
            if max_files and i > max_files:
                break
            executor.submit(download, client, blob, destination_folder, downloaded)
    return downloaded


def download(client, blob, destination_folder, downloaded):
    name, ext = os.path.splitext(blob.name)
    if ext.lower() == '.zip' and os.path.splitext(name)[1].lower() == '.csv':
        file = os.path.join(destination_folder, blob.name)
        blob.download_to_filename(
            filename=file,
            client=client
        )
        with ZipFile(file) as z:
            z.extract(name, path=destination_folder)
        os.remove(file)
        downloaded.append(os.path.join(destination_folder, name))

        logging.info(f"{blob.name}: file {name} was written")
    else:
        logging.info(f"{blob.name}: skipped downloading")


if __name__ == '__main__':
    # os.makedirs('./testupload')
    download_files_from_public_gs_bucket(
        bucket_name='kottestbucket',
        destination_folder='./testupload'
    )
