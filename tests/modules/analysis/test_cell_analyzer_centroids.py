def test_geometric_centroid_is_inside_cell(labeled_ds_with_extras, make_analysis_config):

    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(labeled_ds_with_extras)

    row = df.iloc[0]

    assert 0 <= row["cell_centroid_geom_x"] < labeled_ds_with_extras.sizes["x"]
    assert 0 <= row["cell_centroid_geom_y"] < labeled_ds_with_extras.sizes["y"]


def test_mass_centroid_exists(labeled_ds_with_extras, make_analysis_config):
    from adapt.modules.analysis.module import RadarCellAnalyzer

    config = make_analysis_config()
    analyzer = RadarCellAnalyzer(config)
    df = analyzer.extract(labeled_ds_with_extras)

    assert "cell_centroid_mass_x" in df.columns
    assert "cell_centroid_mass_y" in df.columns
