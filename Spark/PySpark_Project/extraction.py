import logging
import logging.config

logging.config.fileConfig("Properties/configuration/logging.config")
logger = logging.getLogger("Extraction")


def extract_files(data, format, file_dir, split_no, headerReq, compressionType):
    try:
        logger.warning("Starting the extraction of files....")

        data.coalesce(split_no).write.mode("overwrite").format(format).save(file_dir, header=headerReq,comression=compressionType)

    except Exception as e:
        logger.error("An error occured while executing extract_files().See trace ==> ",str(e))
        raise
    else:
        logger.warning("extract_files() method executed successfully...")