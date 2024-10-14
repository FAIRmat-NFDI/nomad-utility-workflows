from nomad.config.models.plugins import ExampleUploadEntryPoint

example_upload_entry_point = ExampleUploadEntryPoint(
    title='New Example Upload',
    category='Examples',
    description='Description of this example upload.',
    path='example_uploads/getting_started',
)
