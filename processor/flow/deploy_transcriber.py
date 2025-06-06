from audio_transcriber import audio_transcribe_file

if __name__ == "__main__":
    audio_transcribe_file.deploy(
        name="audio-transcription-deployment",
        tags=["audio-processing"],
        work_pool_name="my-kubernetes-pool-1",
        description="Deployment for transcribing individual audio files with job cleanup.",
        image="audio-transcription-deployment:v1",
        push=False
    )
    