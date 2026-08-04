"""Generation of dim_grid_cells: the static 1-degree reference grid.

Immutable geographic units, chosen over routes because routes vary daily with weather and
air-traffic control. Also builds the neighbour adjacency the periphery analysis needs, so
"cells adjacent to conflict" is a join rather than runtime geometry.
"""
