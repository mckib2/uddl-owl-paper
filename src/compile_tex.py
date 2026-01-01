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

def count_words(input_file):
    """Count words in LaTeX document, excluding references, appendices, and TOC.
    
    According to INCOSE guidelines:
    - Should not exceed 7,000 words
    - Shall not be less than 2,000 words
    - Excludes: references, appendices, table of contents
    - Includes: exhibits and tables
    """
    if not check_command_available('texcount'):
        print("Warning: texcount not available, skipping word count")
        return None
    
    try:
        import tempfile
        import re
        
        # Read the input file and exclude appendices
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
        
        # Find where appendices start
        # Look for \appendix, \section{Appendix}, or \begin{appendices}
        appendix_patterns = [
            r'\\appendix\b',
            r'\\section\{[^}]*[Aa]ppendix[^}]*\}',
            r'\\begin\{appendices\}',
        ]
        
        appendix_start = None
        for pattern in appendix_patterns:
            match = re.search(pattern, content)
            if match:
                appendix_start = match.start()
                break
        
        # If appendix found, truncate content before it
        if appendix_start is not None:
            content = content[:appendix_start]
            # Also exclude references section if it comes before appendices
            # (though it usually comes after)
        
        # Also exclude references section (\printbibliography or \begin{thebibliography})
        ref_patterns = [
            r'\\printbibliography',
            r'\\begin\{thebibliography\}',
            r'\\section\{[^}]*[Rr]eferences?[^}]*\}',
        ]
        
        ref_start = None
        for pattern in ref_patterns:
            match = re.search(pattern, content)
            if match:
                ref_start = match.start()
                break
        
        if ref_start is not None:
            content = content[:ref_start]
        
        # Create a temporary file with the filtered content
        # Use the same directory as input file to resolve relative paths if any
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tex', delete=False, encoding='utf-8', dir=os.path.dirname(input_file)) as tmp_file:
            # Add texcount directives to ensure tables are counted and metadata is handled correctly
            directives = [
                "%TC:envir table 0 1",
                "%TC:envir table* 0 1",
                "%TC:envir tabular 1 1",
                "%TC:envir tabular* 1 1",
                "%TC:envir tabularx 2 1",
                "%TC:envir longtable 1 1",
                "%TC:macro \\miniheading [1]",
                "%TC:macro \\colfig [0]",
                "%TC:macro \\authorcard [0,0,0,0,0]",
                "%TC:macro \\authorbioentry [0,0,0]"
            ]
            tmp_file.write('\n'.join(directives) + '\n')
            tmp_file.write(content)
            tmp_file_path = tmp_file.name
        
        try:
            # Use texcount to count words
            # -sum: Sum all counts
            # -inc: Include subfiles
            # -nosub: Don't count subcounts separately  
            # -merge: Merge all counts
            # -q: Quiet mode
            cmd = [
                'texcount',
                '-sum',
                '-inc',
                '-nosub',
                '-merge',
                '-q',
                tmp_file_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                output = result.stdout.strip()
                
                # Parse the output - texcount typically outputs something like:
                # "Sum count: 1234"
                # or "Words in text: 1234"
                
                # Try to find the total word count
                patterns = [
                    r'Sum count:\s*(\d+)',
                    r'Words in text:\s*(\d+)',
                    r'Total\s+(\d+)',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, output, re.IGNORECASE)
                    if match:
                        words = int(match.group(1).replace(',', ''))
                        return words
                
                # Fallback: find the last number in the output (usually the total)
                numbers = re.findall(r'\b(\d{1,3}(?:,\d{3})*)\b', output)
                if numbers:
                    # Get the largest number (likely the total)
                    max_num = max(int(n.replace(',', '')) for n in numbers)
                    return max_num
            
            return None
        finally:
            # Clean up temporary file
            try:
                os.unlink(tmp_file_path)
            except OSError:
                pass
        
    except (subprocess.TimeoutExpired, FileNotFoundError, OSError, ValueError, UnicodeDecodeError) as e:
        print(f"Warning: Could not count words: {e}")
        return None

def count_abstract_words(input_file):
    """Count words in the abstract section.
    
    Abstract should not exceed 300 words.
    """
    if not check_command_available('texcount'):
        return None
    
    try:
        import tempfile
        import re
        
        with open(input_file, 'r', encoding='utf-8') as f:
            content = f.read()
            
        # Try to find abstract using \miniheading{Abstract} (used in template)
        # Ends at next \phantomsection, \miniheading, \section, or \subsubsection
        pattern = r'\\miniheading\{Abstract\}(.*?)(?:\\phantomsection|\\miniheading|\\section|\\subsection|\\subsubsection)'
        match = re.search(pattern, content, re.DOTALL)
        
        # Fallback to standard LaTeX abstract environment
        if not match:
            pattern = r'\\begin\{abstract\}(.*?)\\end\{abstract\}'
            match = re.search(pattern, content, re.DOTALL)
            
        if not match:
            return None
            
        abstract_text = match.group(1)
        
        with tempfile.NamedTemporaryFile(mode='w', suffix='.tex', delete=False, encoding='utf-8', dir=os.path.dirname(input_file)) as tmp_file:
            tmp_file.write(abstract_text)
            tmp_file_path = tmp_file.name
            
        try:
            # Use texcount to count words in the snippet
            cmd = [
                'texcount',
                '-sum',
                '-merge',
                '-q',
                tmp_file_path
            ]
            
            result = subprocess.run(cmd, capture_output=True, text=True, timeout=30)
            
            if result.returncode == 0:
                output = result.stdout.strip()
                # Parse output
                patterns = [
                    r'Sum count:\s*(\d+)',
                    r'Words in text:\s*(\d+)',
                    r'Total\s+(\d+)',
                ]
                
                for pattern in patterns:
                    match = re.search(pattern, output, re.IGNORECASE)
                    if match:
                        return int(match.group(1).replace(',', ''))
                        
                # Fallback
                numbers = re.findall(r'\b(\d{1,3}(?:,\d{3})*)\b', output)
                if numbers:
                    return max(int(n.replace(',', '')) for n in numbers)
                    
            return None
        finally:
            try:
                os.unlink(tmp_file_path)
            except OSError:
                pass
                
    except Exception as e:
        print(f"Warning: Could not count abstract words: {e}")
        return None

def compile_pdf(input_file, output_file, build_dir, input_basename, jobname, env, verbose=False):
    """Compile LaTeX to PDF using pdflatex and biber."""
    pdflatex_cwd = build_dir
    if verbose:
        print(f"Running pdflatex in: {pdflatex_cwd}")

    # Use input_file (full path) if available, otherwise input_basename
    tex_input = input_file if os.path.isabs(input_file) else input_basename
    
    # -output-directory is needed if we are running in a different dir than the input file
    # and want output there.
    cmd_pdflatex = ['pdflatex', '-interaction=nonstopmode', '-output-directory', '.', tex_input]
    cmd_biber = ['biber', jobname]
    
    def run_with_output(cmd, step_name, cwd, env, verbose, check_pdf=False):
        """Run command and handle output based on verbose flag."""
        if verbose:
            print(f"--- {step_name} ---")
            result = subprocess.run(cmd, cwd=cwd, env=env, check=False)
        else:
            # Silent mode - only capture output, don't print anything unless there's an error
            result = subprocess.run(cmd, cwd=cwd, env=env, check=False, 
                                  capture_output=True, text=True)
            if result.returncode != 0:
                # Check if PDF was successfully created (for pdflatex runs)
                pdf_created = False
                if check_pdf:
                    pdf_path = os.path.join(cwd, f"{jobname}.pdf")
                    pdf_created = os.path.exists(pdf_path) and os.path.getsize(pdf_path) > 0
                
                # Only show output if PDF was NOT created (actual failure)
                if not pdf_created:
                    # Check if there are actual errors (not just warnings)
                    output = result.stdout + result.stderr
                    has_error = any(keyword in output for keyword in [
                        '! LaTeX Error',
                        '! Undefined control sequence',
                        '! Missing',
                        'Fatal error',
                        'Emergency stop',
                        '! Emergency stop'
                    ])
                    if has_error:
                        # Only print on actual errors when PDF wasn't created
                        print(f"Error during {step_name}:")
                        print(result.stdout)
                        if result.stderr:
                            print(result.stderr)
        return result
    
    # Run pdflatex (1st run)
    result = run_with_output(cmd_pdflatex, "1st pdflatex run", pdflatex_cwd, env, verbose, check_pdf=True)
         
    # Run biber
    result = run_with_output(cmd_biber, "biber run", pdflatex_cwd, env, verbose, check_pdf=False)
    
    # Run pdflatex (2nd run)
    result = run_with_output(cmd_pdflatex, "2nd pdflatex run", pdflatex_cwd, env, verbose, check_pdf=True)
    
    # Run pdflatex (3rd run)
    result = run_with_output(cmd_pdflatex, "3rd pdflatex run", pdflatex_cwd, env, verbose, check_pdf=True)
    
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
        if verbose:
            print(f"PDF moved to: {output_file}")
    elif os.path.exists(output_file):
        if verbose:
            print(f"PDF already at output location: {output_file}")
    else:
        # Check if PDF exists in build_dir (might be named differently)
        if not os.path.exists(pdf_path):
            print(f"Error: PDF not found at {pdf_path} or {output_file}")
            sys.exit(1)

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
    parser.add_argument("-v", "--verbose", action="store_true", 
                       help="Show full output from pdflatex and biber")
    
    args = parser.parse_args()

    input_file = os.path.abspath(args.input_file)
    output_file = os.path.abspath(args.output_file)
    
    if args.verbose:
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
            # Check if resource exists, or if it's in a figures/ subdirectory
            res_abspath = os.path.abspath(res)
            res_dir = os.path.dirname(res_abspath)
            res_basename = os.path.basename(res_abspath)
            
            # Check if there's a figures/ subdirectory with the same file
            figures_path = os.path.join(res_dir, 'figures', res_basename)
            if os.path.exists(figures_path):
                # Use the file from figures/ subdirectory
                source_path = figures_path
                dest_dir = os.path.join(build_dir, 'figures')
                os.makedirs(dest_dir, exist_ok=True)
                dest_path = os.path.join(dest_dir, res_basename)
                if args.verbose:
                    print(f"Copying resource {source_path} to {dest_path} (from figures/ subdirectory)")
                shutil.copy2(source_path, dest_path)
            elif os.path.exists(res):
                 # Copy to build_dir so latex can find it
                 # Preserve directory structure if the resource path includes subdirectories
                 parent_dir = os.path.basename(os.path.dirname(res_abspath))
                 
                 # If the parent directory is not empty and is a meaningful subdirectory
                 # (not just the build root), preserve it
                 if parent_dir and parent_dir not in ('', '.', '..'):
                     # Check if this looks like a subdirectory we should preserve
                     # (e.g., 'figures', 'images', etc.)
                     dest_dir = os.path.join(build_dir, parent_dir)
                     os.makedirs(dest_dir, exist_ok=True)
                     dest_path = os.path.join(dest_dir, res_basename)
                     if args.verbose:
                         print(f"Copying resource {res} to {dest_path} (preserving {parent_dir}/ structure)")
                     shutil.copy2(res, dest_path)
                 else:
                     # Simple file copy to build_dir root
                     if args.verbose:
                         print(f"Copying resource {res} to {build_dir}")
                     shutil.copy2(res, build_dir)
            else:
                 if args.verbose:
                     print(f"Warning: Resource {res} not found")
                 else:
                     print(f"Error: Resource {res} not found")
                     sys.exit(1)

    # Check if LaTeX tools are available
    has_pdflatex = check_command_available('pdflatex')
    has_biber = check_command_available('biber')
    
    if has_pdflatex and has_biber:
        if args.verbose:
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
        
        if args.verbose:
            print(f"TEXINPUTS: {env['TEXINPUTS']}")
        
        compile_pdf(input_file, output_file, build_dir, input_basename, jobname, env, args.verbose)
        
        # Count words and report
        print("\n" + "="*70)
        print("WORD COUNT REPORT")
        print("="*70)
        word_count = count_words(input_file)
        if word_count is not None:
            print(f"Total word count (excluding references, appendices, TOC): {word_count:,}")
            print(f"\nINCOSE Guidelines:")
            print(f"  Minimum: 2,000 words")
            print(f"  Maximum: 7,000 words")
            if word_count < 2000:
                print(f"  ⚠️  WARNING: Document is {2000 - word_count:,} words below minimum!")
            elif word_count > 7000:
                print(f"  ⚠️  WARNING: Document is {word_count - 7000:,} words above maximum!")
            else:
                print(f"  ✓ Document length is within acceptable range")
        else:
            print("Could not determine word count")
            
        # Abstract word count
        abstract_count = count_abstract_words(input_file)
        if abstract_count is not None:
            print(f"\nAbstract word count: {abstract_count:,}")
            print(f"  Maximum: 300 words")
            if abstract_count > 300:
                print(f"  ⚠️  WARNING: Abstract is {abstract_count - 300:,} words above maximum!")
            else:
                print(f"  ✓ Abstract length is within acceptable range")
        
        print("="*70 + "\n")
    else:
        print("LaTeX tools (pdflatex and/or biber) not detected.")
        if not check_command_available('pandoc'):
            print("Error: pandoc is not installed or not in PATH. Please install pandoc first.")
            sys.exit(1)
        
        compile_html(input_file, output_file, build_dir, input_basename)


if __name__ == "__main__":
    main()
