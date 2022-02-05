def _track_log_segments(self):
    log_paths = list(Path(self.topic_dir_path).rglob("*.katar"))

    err = False
    for path in log_paths:
        segment = path.name.split(".")[0]

        try:
            index_file = self.topic_dir_path / f"{segment}.index"
            timeindex_file = self.topic_dir_path / f"{segment}.timeindex"
            assert index_file.is_file()
            assert timeindex_file.is_file()
        except Exception as e:
            message = "Segment file is missing"
            trace = traceback.format_exc()
            error = str(e)
            logger.error(event=message, error=error, stacktrace=trace)
            err = True
            return err

        self.segments.append(int(segment))
    self.segments_count = len(self.segments)
    success = True
    return success
