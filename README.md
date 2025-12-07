# UDDL --> SWT Mapping

## Requirements

- Python 3.8+
- [Meson](https://mesonbuild.com/) (>= 0.60.0) and [Ninja](https://ninja-build.org/)
- LaTeX (with `pdflatex` and `biber`)

## Installation

Install Python dependencies:

```bash
python -m pip install ninja meson-python
```

## Build

Configure and compile the project:

```bash
meson setup build
meson compile -C build paper
```

## Output

The generated paper can be found at:

```
build/main.pdf
```
