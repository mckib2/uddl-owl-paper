project('uddl-owl-mapping', 'c',
  version : '0.1',
  default_options : ['warning_level=3'])

py = import('python').find_installation('python3')

# Define input files
main_tex = files('main.tex')
ref_bib = files('ref.bib')
compile_script = files('src/compile_tex.py')

# Define the PDF compilation target
main_pdf = custom_target(
  'paper',
  input: main_tex,
  output: 'main.pdf',
  command: [
    py, 
    compile_script,
    '@INPUT@', 
    '@OUTPUT@',
    '--build-dir', '@PRIVATE_DIR@'
  ],
  depend_files: [ref_bib],
  build_by_default: true,
  install: false
)

