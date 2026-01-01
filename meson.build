project('uddl-owl-mapping', 'c',
  version : '0.1',
  default_options : ['warning_level=3'])

py = import('python').find_installation('python3')

# Define input files
main_tex = files('main.tex')
ref_bib = files('references.bib')
compile_script = files('src/compile_tex.py')
mmd_file = files('entity-association-explainer.mmd')

# Generate PNG from Mermaid
mermaid_png = custom_target(
  'entity-association-explainer-png',
  input: mmd_file,
  output: 'entity-association.png',
  command: [
    py, '-c',
    'import os, shutil, subprocess, sys; input_file = sys.argv[1]; output_file = sys.argv[2]; outdir = os.path.dirname(output_file); os.makedirs(os.path.join(outdir, "figures"), exist_ok=True); fig_path = os.path.join(outdir, "figures", "entity-association.png"); subprocess.run(["npx", "-p", "@mermaid-js/mermaid-cli", "mmdc", "-i", input_file, "-o", fig_path, "-b", "transparent", "-t", "neutral"], check=True); shutil.copy2(fig_path, output_file)',
    '@INPUT@',
    '@OUTPUT@'
  ]
)

# Generate figures from make_diagrams.sh
visowl_script = files('src/visowl.py')
vistuple_script = files('src/vistuple.py')
spacecraft_owl = files('src/examples/spacecraft.owl')
incose_uddl = files('src/examples/incose_uddl2owl.txt')

# Generate ontology-diagram.mmd
ontology_mmd = custom_target(
  'ontology-diagram-mmd',
  input: [visowl_script, spacecraft_owl],
  output: 'ontology-diagram.mmd',
  command: [
    py,
    visowl_script,
    spacecraft_owl,
    '--output', '@OUTPUT@'
  ],
  build_by_default: false
)

# Generate ontology-diagram.png
# Check for ImageMagick convert command (optional, for trimming)
convert = find_program('convert', required: false)
# Get the output directory for figures
if convert.found()
  ontology_png = custom_target(
    'ontology-diagram-png',
    input: ontology_mmd,
    output: 'ontology-diagram.png',
    command: [
      py, '-c',
      'import os, shutil, subprocess, sys; input_file = sys.argv[1]; output_file = sys.argv[2]; outdir = os.path.dirname(output_file); os.makedirs(os.path.join(outdir, "figures"), exist_ok=True); fig_path = os.path.join(outdir, "figures", "ontology-diagram.png"); subprocess.run(["npx", "-p", "@mermaid-js/mermaid-cli", "mmdc", "-q", "-b", "-t", "neutral", "--scale", "2", "-i", input_file, "-o", fig_path], check=True); subprocess.run(["convert", fig_path, "-trim", fig_path], check=True); shutil.copy2(fig_path, output_file)',
      '@INPUT@',
      '@OUTPUT@'
    ],
    build_by_default: false
  )
else
  ontology_png = custom_target(
    'ontology-diagram-png',
    input: ontology_mmd,
    output: 'ontology-diagram.png',
    command: [
      py, '-c',
      'import os, shutil, subprocess, sys; input_file = sys.argv[1]; output_file = sys.argv[2]; outdir = os.path.dirname(output_file); os.makedirs(os.path.join(outdir, "figures"), exist_ok=True); fig_path = os.path.join(outdir, "figures", "ontology-diagram.png"); subprocess.run(["npx", "-p", "@mermaid-js/mermaid-cli", "mmdc", "-q", "-b", "-t", "neutral", "--scale", "2", "-i", input_file, "-o", fig_path], check=True); shutil.copy2(fig_path, output_file)',
      '@INPUT@',
      '@OUTPUT@'
    ],
    build_by_default: false
  )
endif

# Generate uddl-data-model.mmd
uddl_mmd = custom_target(
  'uddl-data-model-mmd',
  input: [vistuple_script, incose_uddl],
  output: 'uddl-data-model.mmd',
  command: [
    py,
    vistuple_script,
    incose_uddl,
    '--output', '@OUTPUT@'
  ],
  build_by_default: false
)

# Generate uddl-data-model.png
if convert.found()
  uddl_png = custom_target(
    'uddl-data-model-png',
    input: uddl_mmd,
    output: 'uddl-data-model.png',
    command: [
      py, '-c',
      'import os, shutil, subprocess, sys; input_file = sys.argv[1]; output_file = sys.argv[2]; outdir = os.path.dirname(output_file); os.makedirs(os.path.join(outdir, "figures"), exist_ok=True); fig_path = os.path.join(outdir, "figures", "uddl-data-model.png"); subprocess.run(["npx", "-p", "@mermaid-js/mermaid-cli", "mmdc", "-q", "-b", "-t", "neutral", "--scale", "2", "-i", input_file, "-o", fig_path], check=True); subprocess.run(["convert", fig_path, "-trim", fig_path], check=True); shutil.copy2(fig_path, output_file)',
      '@INPUT@',
      '@OUTPUT@'
    ],
    build_by_default: false
  )
else
  uddl_png = custom_target(
    'uddl-data-model-png',
    input: uddl_mmd,
    output: 'uddl-data-model.png',
    command: [
      py, '-c',
      'import os, shutil, subprocess, sys; input_file = sys.argv[1]; output_file = sys.argv[2]; outdir = os.path.dirname(output_file); os.makedirs(os.path.join(outdir, "figures"), exist_ok=True); fig_path = os.path.join(outdir, "figures", "uddl-data-model.png"); subprocess.run(["npx", "-p", "@mermaid-js/mermaid-cli", "mmdc", "-q", "-b", "-t", "neutral", "--scale", "2", "-i", input_file, "-o", fig_path], check=True); shutil.copy2(fig_path, output_file)',
      '@INPUT@',
      '@OUTPUT@'
    ],
    build_by_default: false
  )
endif

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
    '--build-dir', '@PRIVATE_DIR@',
    '--extra-resource', mermaid_png,
    '--extra-resource', ontology_png,
    '--extra-resource', uddl_png
  ],
  depends: [ontology_png, uddl_png],
  depend_files: [ref_bib, mmd_file],
  build_by_default: true,
  install: false
)
