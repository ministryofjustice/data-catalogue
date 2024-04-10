import zipfile
import logging
import glob
import os

logging.getLogger().setLevel(logging.INFO)
file_directory = os.path.dirname(os.path.abspath(__file__))


def zip_data_product(folder: str):
    with zipfile.ZipFile(
        os.path.join(file_directory, f"data_product_{folder}.zip"), "w"
    ) as f:
        for file in (
            glob.glob(os.path.join(file_directory, "application", "*.py"))
            + glob.glob(os.path.join(file_directory, "requirements.txt"))
            + glob.glob(os.path.join(file_directory, "metadata", "*"))
        ):
            logging.info(file)
            f.write(file, arcname=file.split(file_directory)[1])


def main():
    zip_data_product("example_prison_data_product")


if __name__ == "__main__":
    main()
