"""


    Формат Геометрии: В коде выше я предполагаю, что ваш Python-агент внутри parse_generic_xml не просто
    трансформирует точки, а собирает их в строку WKT (например, POLYGON((lon1 lat1, lon2 lat2, ...))).
    Это проще всего передать в SQL.
"""
def to_wkt(contours):
    polys = []
    for c in contours:
        for elem in c['elements']:
            pts = [f"{p['lon']} {p['lat']}" for p in elem] # После трансформации
            polys.append(f"(({', '.join(pts)}))")
    return f"MULTIPOLYGON({', '.join(polys)})" if polys else None