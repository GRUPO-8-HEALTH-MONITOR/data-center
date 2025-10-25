import os
import logging
from datetime import datetime, timedelta

def custom_logger(name: str, file_path: str = None, level: int = logging.DEBUG) -> logging.Logger:
    """
    Create a custom logger that logs to both console and a file.

    Args:
        name (str): Name of the logger.
        file_path (str, optional): Path to the log file. Defaults to None.
        level (int, optional): Logging level. Defaults to logging.DEBUG.

    Returns:
        logging.Logger: The configured logger.
    """    
    logger = logging.getLogger(name)
    logger.setLevel(level)

    logging.getLogger("WDM").setLevel(logging.FATAL)
    logging.getLogger('urllib3').setLevel(logging.WARNING)
    logging.getLogger('tensorflow').setLevel(logging.ERROR)

    logging.getLogger().setLevel(logging.WARNING)

    if not logger.hasHandlers():
        stream_handler = logging.StreamHandler()
        formatter = logging.Formatter('[%(asctime)s] [%(levelname)s] [pid=%(process)d] %(name)s: %(message)s', datefmt='%d/%m/%Y %H:%M:%S')
        stream_handler.setFormatter(formatter)
        logger.addHandler(stream_handler)
        project_root = os.path.dirname(os.path.abspath(__file__))
        log_dir = os.path.join(project_root, '..', '..', 'log')
        os.makedirs(log_dir, exist_ok=True)

        _cleanup_old_logs(log_dir)

        if file_path is None:
            file_path = os.path.join(log_dir, 'log.log')

        file_handler = logging.FileHandler(file_path, encoding='utf-8')
        file_handler.setFormatter(formatter)
        logger.addHandler(file_handler)

    return logger

def _cleanup_old_logs(directory: str, days: int = 10):
    """
    Preserve only the log entries in log.log that are within the last `days`.
    Do not remove any files; only truncate/rewrite log.log keeping recent entries.

    Lines that do not start with a timestamp (continuation lines) are kept only if
    they belong to a log entry whose timestamp is within the cutoff. Lines without
    any preceding timestamp are treated as old and removed.
    """
    cutoff = datetime.now() - timedelta(days=days)
    fpath = os.path.join(directory, 'log.log')

    if not os.path.isfile(fpath):
        return

    kept_lines = []
    last_ts = None
    try:
        with open(fpath, 'r', encoding='utf-8') as f:
            for line in f:
                stripped = line.lstrip()
                if stripped.startswith('['):
                    end = stripped.find(']')
                    if end != -1:
                        ts_str = stripped[1:end]
                        try:
                            ts = datetime.strptime(ts_str, '%d/%m/%Y %H:%M:%S')
                            last_ts = ts
                            if ts >= cutoff:
                                kept_lines.append(line)
                        except Exception:
                            if last_ts and last_ts >= cutoff:
                                kept_lines.append(line)
                    else:
                        if last_ts and last_ts >= cutoff:
                            kept_lines.append(line)
                else:
                    if last_ts and last_ts >= cutoff:
                        kept_lines.append(line)
    except Exception:
        return

    try:
        with open(fpath, 'w', encoding='utf-8') as f:
            f.writelines(kept_lines)
    except Exception:
        pass