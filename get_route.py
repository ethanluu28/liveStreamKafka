import json
import requests

# OSRMs public demo server url string
OSRM_URL = "http://router.project-osrm.org/route/v1/driving/{lon1},{lat1};{lon2},{lat2}"


def get_route(start: tuple[float, float], end: tuple[float, float]) -> list[tuple[float, float]]:
    # start, end: (lat, lon) tuples
    # Returns a list of (lat, lon) tuples following real roads between them.

    url = OSRM_URL.format(lat1=start[0], lon1=start[1], lat2=end[0], lon2=end[1])
    resp = requests.get(url, params={"geometries": "geojson", "overview": "full"}, timeout=10)
    resp.raise_for_status()
    data = resp.json()

    if data.get("code") != "Ok":
        raise RuntimeError(f"OSRM error: {data.get('message', data.get('code'))}")

    # OSRM returns [lon, lat] pairs -- flip to (lat, lon) for everything downstream
    coords = data["routes"][0]["geometry"]["coordinates"]
    return [(lat, lon) for lon, lat in coords]


if __name__ == "__main__":
    # Setting start and end lat/long for simulation
    # (lat, lon) format
    start = (34.1184, -118.3004)  # Griffith Observatory
    end = (34.0195, -118.4912)    # Santa Monica Pier

    route = get_route(start, end)
    print(f"Got {len(route)} points along the route")

    # Saving to json file
    with open("route.json", "w") as f:
        json.dump(route, f)

    print("Saved to route.json")
