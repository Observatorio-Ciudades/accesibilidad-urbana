config = {
    "version": "v1",
    "config": {
        "visState": {
            "filters": [],
            "layers": [
                {
                    "id": "jsx1yd",
                    "type": "geojson",
                    "config": {
                        "dataId": "data",
                        "label": "data",
                        "color": [231, 159, 213],
                        "columns": {"geojson": "geometry"},
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.35,
                            "strokeOpacity": 0.05,
                            "thickness": 0.5,
                            "strokeColor": [28, 27, 27],
                            "colorRange": {
                                "name": "Custom Palette",
                                "type": "custom",
                                "category": "Custom",
                                "colors": [
                                    "#FAE300",
                                    "#FD7900",
                                    "#CF1750",
                                    "#7A0DA6",
                                    "#2C51BE",
                                ],
                            },
                            "strokeColorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#5A1846",
                                    "#900C3F",
                                    "#C70039",
                                    "#E3611C",
                                    "#F1920E",
                                    "#FFC300",
                                ],
                            },
                            "radius": 10,
                            "sizeRange": [0, 10],
                            "radiusRange": [0, 50],
                            "heightRange": [0, 500],
                            "elevationScale": 5,
                            "stroked": True,
                            "filled": True,
                            "enable3d": False,
                            "wireframe": False,
                        },
                        "hidden": False,
                        "textLabel": [
                            {
                                "field": None,
                                "color": [255, 255, 255],
                                "size": 18,
                                "offset": [0, 0],
                                "anchor": "start",
                                "alignment": "center",
                            }
                        ],
                    },
                    "visualChannels": {
                        "colorField": {"name": "dist_hospitales", "type": "real"},
                        "colorScale": "quantile",
                        "sizeField": None,
                        "sizeScale": "linear",
                        "strokeColorField": None,
                        "strokeColorScale": "quantile",
                        "heightField": {"name": "dist_farmacia", "type": "real"},
                        "heightScale": "linear",
                        "radiusField": None,
                        "radiusScale": "linear",
                    },
                }
            ],
            "interactionConfig": {
                "tooltip": {
                    "fieldsToShow": {
                        "data": [{"name": "Distancia a hospitales", "format": None}]
                    },
                    "compareMode": False,
                    "compareType": "absolute",
                    "enabled": True,
                },
                "brush": {"size": 0.5, "enabled": False},
                "geocoder": {"enabled": False},
                "coordinate": {"enabled": False},
            },
            "layerBlending": "normal",
            "splitMaps": [],
            "animationConfig": {"currentTime": None, "speed": 1},
        },
        "mapState": {
            "bearing": 0,
            "dragRotate": False,
            "latitude": 21.865922323269984,
            "longitude": -102.30626392453544,
            "pitch": 0,
            "zoom": 8.515158481972351,
            "isSplit": False,
        },
        "mapStyle": {
            "styleType": "v2qy81l",
            "topLayerGroups": {},
            "visibleLayerGroups": {"label": True, "road": True, "water": True},
            "threeDBuildingColor": [
                9.665468314072013,
                17.18305478057247,
                31.1442867897876,
            ],
            "mapStyles": {
                "a2yomtk": {
                    "accessToken": "pk.eyJ1IjoibmF0ZXJhIiwiYSI6ImNqNXRxcTdsMDFsamUycXBoOTd5NHZxOXYifQ.c3g6rKBuuK3urwqlQEkO2g",
                    "custom": True,
                    "icon": "https://api.mapbox.com/styles/v1/natera/ckuqusx1a0q9218pcb5z1uvcq/static/-122.3391,37.7922,9,0,0/400x300?access_token=pk.eyJ1IjoibmF0ZXJhIiwiYSI6ImNqNXRxcTdsMDFsamUycXBoOTd5NHZxOXYifQ.c3g6rKBuuK3urwqlQEkO2g&logo=false&attribution=false",
                    "id": "a2yomtk",
                    "label": "Monochrome",
                    "url": "mapbox://styles/natera/ckuqusx1a0q9218pcb5z1uvcq",
                },
            },
        },
    },
}

config_idx = {
    "version": "v1",
    "config": {
        "visState": {
            "filters": [],
            "layers": [
                {
                    "id": "jsx1yd",
                    "type": "geojson",
                    "config": {
                        "dataId": "data",
                        "label": "data",
                        "color": [231, 159, 213],
                        "columns": {"geojson": "geometry"},
                        "isVisible": True,
                        "visConfig": {
                            "opacity": 0.35,
                            "strokeOpacity": 0.05,
                            "thickness": 0.5,
                            "strokeColor": [28, 27, 27],
                            "colorRange": {
                                "name": "Custom Palette",
                                "type": "custom",
                                "category": "Custom",
                                "colors": [
                                    "#2C51BE",
                                    "#7A0DA6",
                                    "#CF1750",
                                    "#FD7900",
                                    "#FAE300",
                                ],
                                "reversed": True,
                            },
                            "strokeColorRange": {
                                "name": "Global Warming",
                                "type": "sequential",
                                "category": "Uber",
                                "colors": [
                                    "#5A1846",
                                    "#900C3F",
                                    "#C70039",
                                    "#E3611C",
                                    "#F1920E",
                                    "#FFC300",
                                ],
                            },
                            "radius": 10,
                            "sizeRange": [0, 10],
                            "radiusRange": [0, 50],
                            "heightRange": [0, 500],
                            "elevationScale": 5,
                            "stroked": True,
                            "filled": True,
                            "enable3d": False,
                            "wireframe": False,
                        },
                        "hidden": False,
                        "textLabel": [
                            {
                                "field": None,
                                "color": [255, 255, 255],
                                "size": 18,
                                "offset": [0, 0],
                                "anchor": "start",
                                "alignment": "center",
                            }
                        ],
                    },
                    "visualChannels": {
                        "colorField": {
                            "name": "bins_idx_accessibility",
                            "type": "string",
                        },
                        "colorScale": "ordinal",
                        "sizeField": None,
                        "sizeScale": "linear",
                        "strokeColorField": None,
                        "strokeColorScale": "quantile",
                        "heightField": {"name": "dist_farmacia", "type": "real"},
                        "heightScale": "linear",
                        "radiusField": None,
                        "radiusScale": "linear",
                    },
                }
            ],
            "interactionConfig": {
                "tooltip": {
                    "fieldsToShow": {
                        "data": [{"name": "idx_accessibility", "format": None}]
                    },
                    "compareMode": False,
                    "compareType": "absolute",
                    "enabled": True,
                },
                "brush": {"size": 0.5, "enabled": False},
                "geocoder": {"enabled": False},
                "coordinate": {"enabled": False},
            },
            "layerBlending": "normal",
            "splitMaps": [],
            "animationConfig": {"currentTime": None, "speed": 1},
        },
        "mapState": {
            "bearing": 0,
            "dragRotate": False,
            "latitude": 21.73127093107669,
            "longitude": -102.36507230084396,
            "pitch": 0,
            "zoom": 9.61578119574967,
            "isSplit": False,
        },
        "mapStyle": {
            "styleType": "dark",
            "topLayerGroups": {},
            "visibleLayerGroups": {
                "label": True,
                "road": True,
                "border": False,
                "building": True,
                "water": True,
                "land": True,
                "3d building": False,
            },
            "threeDBuildingColor": [
                9.665468314072013,
                17.18305478057247,
                31.1442867897876,
            ],
            "mapStyles": {
                "a2yomtk": {
                    "accessToken": "pk.eyJ1IjoibmF0ZXJhIiwiYSI6ImNqNXRxcTdsMDFsamUycXBoOTd5NHZxOXYifQ.c3g6rKBuuK3urwqlQEkO2g",
                    "custom": True,
                    "icon": "https://api.mapbox.com/styles/v1/natera/ckuqusx1a0q9218pcb5z1uvcq/static/-122.3391,37.7922,9,0,0/400x300?access_token=pk.eyJ1IjoibmF0ZXJhIiwiYSI6ImNqNXRxcTdsMDFsamUycXBoOTd5NHZxOXYifQ.c3g6rKBuuK3urwqlQEkO2g&logo=false&attribution=false",
                    "id": "a2yomtk",
                    "label": "Monochrome",
                    "url": "mapbox://styles/natera/ckuqusx1a0q9218pcb5z1uvcq",
                },
            },
        },
    },
}
