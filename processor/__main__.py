from prefect import flow, task, get_run_logger, get_client
import asyncio
from pathlib import Path
import ffmpeg
from pydub import AudioSegment
import whisper # Ensure you have this installed: pip install openai-whisper
import os
from processor.workpool.work_pool import create_work_pool

# --- Dummy file setup (for local testing) ---
# This simulates the file you'd be processing
def create_dummy_audio_file(filename="Nums_5dot1_24_48000.wav"):
    if not os.path.exists("processor/files"):
        os.makedirs("processor/files")
    filepath = Path("processor/files") / filename
    if not filepath.exists():
        print(f"Creating dummy audio file: {filepath} for testing...")
        # Create a simple 2-channel audio (stereo) for splitting demo
        from scipy.io.wavfile import write
        import numpy as np
        samplerate = 44100
        duration = 2.0
        stereo_data = np.random.uniform(-0.1, 0.1, (int(samplerate * duration), 2)).astype(np.float32)
        write(filepath, samplerate, stereo_data)
        print("Dummy audio file created.")
    else:
        print(f"Dummy audio file '{filepath}' already exists.")

# --- Prefect Tasks ---

@task(name="Check File exists")
async def check_file(Process_File: str) -> bool:
    logger = get_run_logger()
    file_path = Path("processor/files/") / Process_File
    if file_path.exists():
        logger.info(f"File '{file_path}' exists.")
        return True
    else:
        logger.warning(f"File '{file_path}' does NOT exist.")
        return False

@task(name="Get Audio Channel Info")
async def get_audio_channel(File: str) -> int:
    """
    Identifies the number of audio channels in a media file using ffprobe.
    Returns the number of channels, or -1 if no audio streams found or an error occurs.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Initiating channel check for: {File}")
        probe = ffmpeg.probe(File)
        audio_streams = [s for s in probe["streams"] if s['codec_type'] == 'audio']

        if not audio_streams:
            logger.info("No audio stream found.")
            return -1

        first_audio_stream = audio_streams[0]
        channels = first_audio_stream.get('channels')

        if channels is not None:
            logger.info(f"Found {channels} channel(s) for {File}.")
            return channels
        else:
            logger.info("Could not determine channels from audio stream metadata.")
            return -1 # Indicate unknown/error state
    except ffmpeg.Error as e:
        logger.error(f"FFmpeg error while checking channels for {File}: {e.stderr.decode('utf-8').strip()}")
        return -1
    except Exception as e:
        logger.error(f"An unexpected error occurred while checking channels for {File}: {e}")
        return -1

@task(name="Split Audio File based on the Channel")
async def split_audio_file(File: str, OutPutDir: str) -> list[str]:
    """
    Splits a multi-channel audio file into individual mono files for each channel.
    Returns a list of paths to the newly created mono files.
    """
    logger = get_run_logger()
    output_files = []
    
    if not File: # Ensure file path is provided
        logger.warning("No input file provided for splitting.")
        return output_files

    output_path_obj = Path(OutPutDir)
    if not output_path_obj.exists():
        os.makedirs(output_path_obj)
        logger.info(f"Created output directory: {output_path_obj}")

    try:
        logger.info(f"Starting to split file: {File}")
        audio_segment = AudioSegment.from_file(file=File)
        num_channels = audio_segment.channels

        if num_channels == 1:
            logger.info("Input file is already mono. Skipping channel splitting.")
            # Optionally, copy the original file to the output directory if it's already mono
            original_file_basename = Path(File).name
            output_single_path = output_path_obj / original_file_basename
            audio_segment.export(output_single_path, format="wav")
            output_files.append(str(output_single_path))
            return output_files

        for channel_idx in range(num_channels):
            logger.info(f"Extracting channel {channel_idx + 1}...")
            mono_channel = audio_segment.split_to_mono()[channel_idx]

            # Use more descriptive names if applicable, otherwise just channel_idx
            channel_name_suffix = f"channel_{channel_idx+1}"
            if num_channels == 2:
                channel_name_suffix = "left" if channel_idx == 0 else "right"
            elif num_channels == 6: # Common 5.1 layout
                channel_map = {0: "front_left", 1: "front_right", 2: "center",
                               3: "lfe", 4: "surround_left", 5: "surround_right"}
                channel_name_suffix = channel_map.get(channel_idx, channel_name_suffix)

            original_basename_no_ext = Path(File).stem
            output_filename = f"{original_basename_no_ext}_{channel_name_suffix}.wav"
            output_full_path = output_path_obj / output_filename
            
            mono_channel.export(output_full_path, format="wav")
            logger.info(f"Exported split file: {output_full_path}")
            output_files.append(str(output_full_path))

        return output_files
    except Exception as e:
        logger.error(f"Error while splitting file '{File}': {e}")
        return [] # Return empty list on error

@task(name="Audio Transcript")
async def audio_transcript(file_path: str) -> str:
    """
    Transcribes an audio file using the Whisper model.
    """
    logger = get_run_logger()
    try:
        logger.info(f"Transcribing file: {file_path}")
        
        # Load the Whisper model. Consider loading this outside the task if memory is an issue
        # for many parallel runs, or use a DaskTaskRunner.
        # model = whisper.load_model("small")
        # result = model.transcribe(file_path)
        
        # transcription = result.get("text", "").strip()
        try:
            from prefect.deployments import run_deployment
            import time
            import traceback
            
            flow_run = await run_deployment(
                # CORRECTED LINE BELOW:
                name="bbe52843-3da6-467e-9811-615eb9543225", # <--- Use FLOW_NAME/DEPLOYMENT_NAME format
                parameters={"audio_path": file_path},
                job_variables={"api_url": "http://localhost:4200/api"},
            )
            if flow_run is None:
                raise ValueError("Flow run was not created. Please check deployment name or Prefect API.")
            client = get_client()
            final_flow_run_state = await client.wait_for_flow_run(flow_run.id)
            if final_flow_run_state.is_completed() and final_flow_run_state.result:
                try:
                    transcription = await client.get_flow_run_result(final_flow_run_state.state_details.result_flow_run_state_id)
                    logger.info(f"Transcription received from child flow for {file_path}: {transcription[:100]}...")
                    return transcription
                except Exception as result_e:
                    logger.error(f"Could not retrieve result from child flow for {file_path}: {result_e}")
                    return f"Transcription failed for {file_path}: Could not retrieve result."
            elif final_flow_run_state.is_failed():
                logger.error(f"Child flow for {file_path} failed. State: {final_flow_run_state.name} - {final_flow_run_state.message}")
                return f"Transcription failed for {file_path}: Child flow failed."
            else:
                logger.warning(f"Child flow for {file_path} finished with unexpected state: {final_flow_run_state.name}")
                return f"Transcription failed for {file_path}: Unexpected child flow state."

            print(f"[{time.strftime('%H:%M:%S')}] Flow run initiated with ID: {flow_run.id} for {file_path}")
            
        except Exception as e:
            print(f"[{time.strftime('%H:%M:%S')}] Error triggering flow run for {file_path}: {e}")
            traceback.print_exc()
        
       
        logger.info(f"Transcription complete for: {file_path}")
        return flow_run
    except Exception as e:
        logger.error(f"Audio Transcription Error for {file_path}: {e}")
        return f"ERROR: Could not transcribe {file_path}"

# --- Prefect Flow ---

@flow(name="Audio Processor Flow")
async def audio_processor_flow(Process_File: str = "Nums_5dot1_24_48000.wav"):
    logger = get_run_logger()
    logger.info(f"Starting audio processing for: {Process_File}")

    # Set up paths
    input_file_path = Path("processor/files/") / Process_File
    output_split_dir = Path("processor/files/Output/Splits") # More specific output directory

    # 1. Check if the file exists
    file_exists = await check_file(Process_File=Process_File)

    if not file_exists:
        logger.error(f"Input file '{input_file_path}' does not exist. Exiting flow.")
        return

    # 2. Identify the number of channels
    channel_count = await get_audio_channel(File=str(input_file_path))

    if channel_count == -1:
        logger.error(f"Could not determine audio channels for '{input_file_path}'. Exiting flow.")
        return

    logger.info(f"Audio file has {channel_count} channel(s).")

    # 3. Split audio if multi-channel, otherwise process the original
    files_to_transcribe: list[str] = []
    if channel_count > 1:
        logger.info("File is multi-channel. Splitting into individual channels...")
        splited_files_futures = await split_audio_file(str(input_file_path), str(output_split_dir))
        # Ensure the result is awaited as split_audio_file is an async task
        splited_files = splited_files_futures # split_audio_file already returns the list directly
        
        if not splited_files:
            logger.error("Failed to split audio file. Exiting transcription process.")
            return

        logger.info(f"Successfully split into {len(splited_files)} files: {splited_files}")
        files_to_transcribe = splited_files
    else:
        logger.info("File is mono. Transcribing the original file directly.")
        files_to_transcribe.append(str(input_file_path)) # Transcribe the original mono file

    # 4. Transcribe files in parallel using .map()
    if files_to_transcribe:
        
        # Check the workpool 
        work_pool_name = "my-kubernetes-pool-1"
        work_pool = await create_work_pool(work_pool_name=work_pool_name)
        
        
        
        logger.info(f"Initiating transcription for {len(files_to_transcribe)} files...")
        # This is the correct way to use .map() on a Prefect Task
        transcript_futures = audio_transcript.map(files_to_transcribe)
        
        # Await all transcription results
        transcribed_texts = [future.result() for future in transcript_futures]
        
        logger.info("\n--- All Transcriptions Completed ---")
        for i, text in enumerate(transcribed_texts):
            logger.info(f"Transcription for {files_to_transcribe[i]}:")
            logger.info(f"-> {text[:100]}...") # Log first 100 chars
    else:
        logger.warning("No files to transcribe.")




# --- Main execution block ---
if __name__ == "__main__":
    # Create dummy file for testing
    create_dummy_audio_file()

    # Clean up previous split outputs before running
    output_split_dir = Path("processor/files/Output/Splits")
    if output_split_dir.exists():
        import shutil
        shutil.rmtree(output_split_dir)
        print(f"Cleaned up previous split directory: {output_split_dir}")

    # Run the Prefect flow
    asyncio.run(audio_processor_flow("Nums_5dot1_24_48000.wav"))

