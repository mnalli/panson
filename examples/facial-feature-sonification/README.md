# Facial Feature Sonification

## OpenFace
- What is [OpenFace](https://github.com/TadasBaltrusaitis/OpenFace)

```sh
# run containerized environment
docker compose up -d

# launch feature extraction of video
docker compose exec openface build/bin/FeatureExtraction -out_dir files/processed -f files/video.avi

# launch realtime feature extraction from the main webcam
docker compose exec openface build/bin/FeatureExtraction -device 0 -pose -gaze -aus -of files/features.csv
```
