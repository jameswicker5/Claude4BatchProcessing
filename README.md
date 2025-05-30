# Claude4 Batch Geolocation Processing

This project contains tools and data for processing geolocation predictions from Claude 4 vision-language model on a 4K benchmark image dataset.

## Folder Structure

- Download the 4K image dataset here: 

https://www.kaggle.com/datasets/jameswicker5/4k-benchmark-images

- `filename_mapping.json`: Maps `custom_id` from Claude's output to actual image filenames.
- `geolocation_results.jsonl`: Claude 4's output in JSONL format containing predictions with reasoning and latitude/longitude.
- `claude4_benchmark_4k.csv`: Final processed CSV file with mapped predictions, true coordinates, and model predictions.
- `errors/`: (Optional) Directory to store errors or mismatches during processing.

## Usage

You can load and parse `geolocation_results.jsonl` and `filename_mapping.json` using Python to match predictions with actual image files and compare true vs. predicted coordinates. Use Haversine distance calculations to evaluate model performance.

## Example Haversine Code (Python)

```python
from math import radians, sin, cos, sqrt, asin

def haversine(lat1, lon1, lat2, lon2):
    R = 6371  # Earth radius in km
    dlat = radians(lat2 - lat1)
    dlon = radians(lon2 - lon1)
    a = sin(dlat / 2)**2 + cos(radians(lat1)) * cos(radians(lat2)) * sin(dlon / 2)**2
    return 2 * R * asin(sqrt(a))
```

## License

This project is for educational and benchmarking purposes only.
