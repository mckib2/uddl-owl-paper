#!/bin/bash

# Get the directory of this script
DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )"

# Create the output directory if it doesn't exist
mkdir -p "$DIR/figures"

# # Generate the entity-association-explainer.png diagram
# python "$DIR/src/vistuple.py" "$DIR/src/examples/satellite.txt" --output "$DIR/src/figures/entity-association-explainer.mmd"
# npx -p @mermaid-js/mermaid-cli mmdc -i "$DIR/src/figures/entity-association-explainer.mmd" -o "$DIR/src/figures/entity-association-explainer.png"

# Generate the ontology-diagram.png diagram
python "$DIR/src/visowl.py" "$DIR/src/examples/spacecraft.owl" --output "$DIR/figures/ontology-diagram.mmd"
npx -p @mermaid-js/mermaid-cli mmdc -q -b -t neutral --scale 2 -i "$DIR/figures/ontology-diagram.mmd" -o "$DIR/figures/ontology-diagram.png"
convert "$DIR/figures/ontology-diagram.png" -trim "$DIR/figures/ontology-diagram.png"

# Generate the uddl-data-model.png diagram
python "$DIR/src/vistuple.py" "$DIR/src/examples/satellite.txt" --output "$DIR/figures/uddl-data-model.mmd"
npx -p @mermaid-js/mermaid-cli mmdc -q -b -t neutral --scale 2 -i "$DIR/figures/uddl-data-model.mmd" -o "$DIR/figures/uddl-data-model.png"
convert "$DIR/figures/uddl-data-model.png" -trim "$DIR/figures/uddl-data-model.png"
