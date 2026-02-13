# Uploading Multiple Files

Build a deck incrementally by uploading multiple CSV + YAML pairs:

1. Upload first pair → first set of blocks renders
2. Upload second pair → blocks append to the bottom
3. Repeat as needed
4. Print to PDF when done

Set `block_starting_number` in subsequent YAML files to maintain
continuous numbering. If the first config renders 8 blocks, set
`block_starting_number: 9` in the second.
