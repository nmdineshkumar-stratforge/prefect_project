from prefect import flow, get_run_logger
import os
import time

@flow(name="Audio-Transcribe", log_prints=True)
def audio_transcribe_file(audio_path: str):
    
    logger = get_run_logger()
    logger.info(f"Starting transcription for audio file: {audio_path}")
    
    if not os.path.exists(audio_path):
        logger.error(f"Audio file not found: {audio_path}")
        raise FileNotFoundError(f"Audio file not found at {audio_path}")
    time.sleep(5)
    transcript_content = f"Transcript for {os.path.basename(audio_path)}: This is a simulated transcription."
    logger.info(f"Finished transcription for {audio_path}. Content snippet: {transcript_content[:50]}...")
    
    return transcript_content

if __name__ == "__main__":
    audio_transcribe_file(audio_path="./sample_audio.mp3")