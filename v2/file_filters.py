def file_exists(workflow_output_file):
    return not bool(workflow_output_file.get('is_deleted', False))


def filter_deleted_files(workflow_outputs):
    return filter(file_exists, workflow_outputs)
