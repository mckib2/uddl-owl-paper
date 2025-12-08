import argparse
import sys
import subprocess
import os
import shutil

def check_command_available(command):
    """Check if a command is available in PATH."""
    try:
        # Try --version first, fall back to --help if that fails
        result = subprocess.run(
            [command, '--version'],
            capture_output=True,
            text=True,
            timeout=5
        )
        if result.returncode == 0:
            return True
        # Some commands might use --help instead
        result = subprocess.run(
            [command, '--help'],
            capture_output=True,
            text=True,
            timeout=5
        )
        return result.returncode == 0
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError):
        return False

def run_command(command):
    print(f"Running: {' '.join(command)}")
    result = subprocess.run(command, check=False, capture_output=True, text=True)
    if result.returncode != 0:
        print(f"Error running command: {' '.join(command)}")
        print(result.stdout)
        print(result.stderr)
        return False
    return True

def compile_pdf(input_file, output_file, build_dir, input_basename, jobname, env):
    """Compile LaTeX to PDF using pdflatex and biber."""
    pdflatex_cwd = build_dir
    print(f"Running pdflatex in: {pdflatex_cwd}")

    # Use input_file (full path) if available, otherwise input_basename
    tex_input = input_file if os.path.isabs(input_file) else input_basename
    
    # -output-directory is needed if we are running in a different dir than the input file
    # and want output there.
    cmd_pdflatex = ['pdflatex', '-interaction=nonstopmode', '-output-directory', '.', tex_input]
    cmd_biber = ['biber', jobname]
    
    # Run pdflatex (1st run)
    print("--- 1st pdflatex run ---")
    subprocess.run(cmd_pdflatex, cwd=pdflatex_cwd, env=env, check=True)
         
    # Run biber
    print("--- biber run ---")
    subprocess.run(cmd_biber, cwd=pdflatex_cwd, env=env, check=True)
    
    # Run pdflatex (2nd run)
    print("--- 2nd pdflatex run ---")
    subprocess.run(cmd_pdflatex, cwd=pdflatex_cwd, env=env, check=True)
    
    # Run pdflatex (3rd run)
    print("--- 3rd pdflatex run ---")
    subprocess.run(cmd_pdflatex, cwd=pdflatex_cwd, env=env, check=True)
    
    # Move PDF to output location if it exists
    pdf_path = os.path.join(build_dir, f"{jobname}.pdf")
    if os.path.exists(pdf_path) and os.path.abspath(pdf_path) != os.path.abspath(output_file):
        # Ensure output directory exists
        output_dir = os.path.dirname(output_file)
        if output_dir and not os.path.exists(output_dir):
            os.makedirs(output_dir, exist_ok=True)
        shutil.copy2(pdf_path, output_file) # Use copy instead of move to keep build artifact? Or move? 
        # Original script used move.
        shutil.move(pdf_path, output_file)
        print(f"PDF moved to: {output_file}")

def compile_html(input_file, output_file, build_dir, input_basename):
    """Compile LaTeX to HTML using pandoc."""
    print("Using pandoc to generate HTML instead of PDF")
    
    # Check if ref.bib exists in build directory or BIBINPUTS
    # For now assume it's findable or passed via args? 
    # The original script looked for ref.bib in build_dir.
    # If we are using source files, ref.bib might be in source dir.
    # Pandoc needs explicit bibliography path.
    
    # We'll search for ref.bib in:
    # 1. build_dir
    # 2. dirname of input_file
    
    ref_bib_candidates = [
        os.path.join(build_dir, "ref.bib"),
        os.path.join(os.path.dirname(input_file), "ref.bib")
    ]
    
    ref_bib = None
    for cand in ref_bib_candidates:
        if os.path.exists(cand):
            ref_bib = cand
            break
            
    if ref_bib:
        print(f"Using bibliography: {ref_bib}")
    else:
        print(f"Warning: ref.bib not found in {ref_bib_candidates}")
    
    # Use the input file directly
    tex_file_to_use = input_file
    
    # Build pandoc command
    pandoc_cmd = ['pandoc', tex_file_to_use, '--standalone', '--mathjax']
    
    if ref_bib:
        pandoc_cmd.extend(['--bibliography', ref_bib, '--citeproc'])
    
    # Determine output file extension
    if output_file.endswith('.pdf'):
        # Change output to HTML if output was PDF
        output_file = os.path.abspath(os.path.splitext(output_file)[0] + '.html')
        print(f"Output changed to HTML: {output_file}")
    
    # Ensure output directory exists
    output_dir = os.path.dirname(output_file)
    if output_dir and not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)
    
    pandoc_cmd.extend(['--output', output_file])
    
    print(f"Converting {tex_file_to_use} to HTML...")
    print(f"Running: {' '.join(pandoc_cmd)}")
    
    result = subprocess.run(pandoc_cmd, cwd=build_dir, check=False)
    
    if result.returncode == 0:
        print(f"Successfully converted to {output_file}")
    else:
        print(f"Error: pandoc conversion failed with exit code {result.returncode}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(description="Compile LaTeX to PDF or HTML.")
    parser.add_argument("input_file", help="Input LaTeX file")
    parser.add_argument("output_file", help="Output PDF file")
    parser.add_argument("--build-dir", help="Directory to run build in", default=None)
    parser.add_argument("--extra-resource", action="append", help="Extra files to make available to latex")
    
    args = parser.parse_args()

    input_file = os.path.abspath(args.input_file)
    output_file = os.path.abspath(args.output_file)
    
    print(f"Input file: {input_file}")
    print(f"Output file: {output_file}")

    input_basename = os.path.basename(input_file)
    jobname = os.path.splitext(input_basename)[0]
    
    # Get the build directory
    # If explicitly provided, use it. Otherwise derive from input file.
    if args.build_dir:
        build_dir = os.path.abspath(args.build_dir)
    else:
        build_dir = os.path.dirname(input_file) or os.getcwd()
    
    if not os.path.exists(build_dir):
        os.makedirs(build_dir, exist_ok=True)

    if args.extra_resource:
        for res in args.extra_resource:
            if os.path.exists(res):
                 # Copy to build_dir so latex can find it
                 print(f"Copying resource {res} to {build_dir}")
                 shutil.copy2(res, build_dir)
            else:
                 print(f"Warning: Resource {res} not found")

    # Check if LaTeX tools are available
    has_pdflatex = check_command_available('pdflatex')
    has_biber = check_command_available('biber')
    
    if has_pdflatex and has_biber:
        print("LaTeX tools (pdflatex and biber) detected. Using PDF compilation.")
        # Update env
        env = os.environ.copy()
        path_sep = os.pathsep
        
        # Add input_file directory to TEXINPUTS/BIBINPUTS so local inputs are found
        # (e.g. if input_file is /src/main.tex, allow \input{other.tex} in /src/)
        source_dir = os.path.dirname(input_file)
        
        # Also add build_dir to TEXINPUTS (for generated files if they are there)
        # Ensure we preserve system paths by appending a separator if one is not present at the end
        current_texinputs = env.get('TEXINPUTS', '')
        if current_texinputs and not current_texinputs.endswith(path_sep):
            current_texinputs += path_sep
        
        env['TEXINPUTS'] = f".{path_sep}{build_dir}{path_sep}{source_dir}{path_sep}" + current_texinputs

        current_bibinputs = env.get('BIBINPUTS', '')
        if current_bibinputs and not current_bibinputs.endswith(path_sep):
            current_bibinputs += path_sep

        env['BIBINPUTS'] = f".{path_sep}{build_dir}{path_sep}{source_dir}{path_sep}" + current_bibinputs
        
        print(f"TEXINPUTS: {env['TEXINPUTS']}")
        
        compile_pdf(input_file, output_file, build_dir, input_basename, jobname, env)
    else:
        print("LaTeX tools (pdflatex and/or biber) not detected.")
        if not check_command_available('pandoc'):
            print("Error: pandoc is not installed or not in PATH. Please install pandoc first.")
            sys.exit(1)
        
        compile_html(input_file, output_file, build_dir, input_basename)


if __name__ == "__main__":
    main()
