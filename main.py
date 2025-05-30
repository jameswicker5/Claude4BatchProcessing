import os
import json
import base64
import hashlib
from anthropic import Anthropic
import time

# Initialize the Anthropic client
client = Anthropic(api_key="your-api-key-here")

def encode_image_to_base64(image_path):
    """Convert image file to base64 string"""
    with open(image_path, "rb") as image_file:
        return base64.b64encode(image_file.read()).decode('utf-8')

def get_image_media_type(file_path):
    """Determine media type based on file extension"""
    ext = os.path.splitext(file_path)[1].lower()
    if ext in ['.jpg', '.jpeg']:
        return "image/jpeg"
    elif ext == '.png':
        return "image/png"
    elif ext == '.gif':
        return "image/gif" 
    elif ext == '.webp':
        return "image/webp"
    else:
        return "image/jpeg"  # default

def create_short_custom_id(filename, index):
    """Create a short custom_id that stays under 64 characters"""
    # Use first 8 characters of MD5 hash of filename for uniqueness
    filename_hash = hashlib.md5(filename.encode()).hexdigest()[:8]
    custom_id = f"geo_{index}_{filename_hash}"
    
    # If still too long, truncate further
    if len(custom_id) > 64:
        custom_id = f"geo_{index}_{filename_hash[:8]}"
    
    return custom_id

def create_batch_requests(image_folder_path):
    """Create batch requests for all images in folder"""
    requests = []
    filename_mapping = {}  # To track custom_id to filename mapping
    
    # Get all image files from the folder
    image_extensions = {'.jpg', '.jpeg', '.png', '.gif', '.webp'}
    image_files = [f for f in os.listdir(image_folder_path) 
                   if os.path.splitext(f)[1].lower() in image_extensions]
    
    print(f"Found {len(image_files)} images to process")
    
    for i, filename in enumerate(image_files):
        image_path = os.path.join(image_folder_path, filename)
        
        try:
            # Encode image
            base64_image = encode_image_to_base64(image_path)
            media_type = get_image_media_type(image_path)
            
            # Create short custom_id
            custom_id = create_short_custom_id(filename, i)
            
            # Store mapping for later reference
            filename_mapping[custom_id] = filename
            
            # Create the request
            request = {
                "custom_id": custom_id,  # Now guaranteed to be under 64 chars
                "params": {
                    "model": "claude-sonnet-4-20250514",
                    "max_tokens": 500,
                    "messages": [
                        {
                            "role": "user",
                            "content": [
                                {
                                    "type": "image",
                                    "source": {
                                        "type": "base64",
                                        "media_type": media_type,
                                        "data": base64_image
                                    }
                                },
                                {
                                    "type": "text",
                                    "text": "You are a geolocation AI trained to estimate the latitude and longitude of any image based on visual features alone — such as architecture, vegetation, signage, weather, and landforms. Even without GPS or metadata, you must always provide your best guess. Please return the result in only this format with 4 decimal places: Latitude: <decimal> Longitude: <decimal>"
                                }
                            ]
                        }
                    ]
                }
            }
            
            requests.append(request)
            
        except Exception as e:
            print(f"Error processing {filename}: {e}")
            continue
    
    # Save filename mapping for later use
    with open("filename_mapping.json", "w") as f:
        json.dump(filename_mapping, f, indent=2)
    print(f"📁 Filename mapping saved to filename_mapping.json")
    
    return requests

def submit_batch_chunks(requests, chunk_size=50):
    """Submit requests in smaller chunks to avoid 256MB limit"""
    batch_ids = []
    total_requests = len(requests)
    
    print(f"Splitting {total_requests} requests into chunks of {chunk_size}")
    
    for i in range(0, total_requests, chunk_size):
        chunk = requests[i:i + chunk_size]
        chunk_num = (i // chunk_size) + 1
        total_chunks = (total_requests + chunk_size - 1) // chunk_size
        
        print(f"\nSubmitting chunk {chunk_num}/{total_chunks} ({len(chunk)} requests)...")
        
        # Debug: Check custom_id lengths in this chunk
        max_id_length = max(len(req['custom_id']) for req in chunk)
        print(f"   Max custom_id length in chunk: {max_id_length}")
        
        try:
            # Create the batch
            batch = client.beta.messages.batches.create(
                requests=chunk
            )
            
            print(f"✅ Chunk {chunk_num} submitted successfully!")
            print(f"   Batch ID: {batch.id}")
            print(f"   Status: {batch.processing_status}")
            
            batch_ids.append({
                'batch_id': batch.id,
                'chunk_num': chunk_num,
                'request_count': len(chunk),
                'start_index': i,
                'end_index': i + len(chunk) - 1
            })
            
            # Small delay between submissions
            time.sleep(1)
            
        except Exception as e:
            print(f"❌ Error creating chunk {chunk_num}: {e}")
            continue
    
    return batch_ids

def submit_batch(requests, batch_name="geolocation_batch"):
    """Legacy function - now redirects to chunked submission"""
    return submit_batch_chunks(requests)

def check_all_batches_status(batch_ids):
    """Check status of all batch chunks"""
    print("Checking status of all batches...\n")
    
    total_succeeded = 0
    total_failed = 0
    total_processing = 0
    all_ended = True
    
    for batch_info in batch_ids:
        batch_id = batch_info['batch_id']
        chunk_num = batch_info['chunk_num']
        
        try:
            batch = client.beta.messages.batches.retrieve(batch_id)
            print(f"Chunk {chunk_num} (ID: {batch_id[:12]}...):")
            print(f"  Status: {batch.processing_status}")
            
            if hasattr(batch, 'request_counts'):
                counts = batch.request_counts
                succeeded = counts.get('succeeded', 0)
                failed = counts.get('failed', 0)
                processing = counts.get('processing', 0)
                
                print(f"  Succeeded: {succeeded}")
                print(f"  Failed: {failed}")
                print(f"  Processing: {processing}")
                
                total_succeeded += succeeded
                total_failed += failed
                total_processing += processing
            
            if batch.processing_status != "ended":
                all_ended = False
                
            print()
            
        except Exception as e:
            print(f"Error checking batch {batch_id}: {e}")
            all_ended = False
    
    print(f"TOTALS:")
    print(f"  Succeeded: {total_succeeded}")
    print(f"  Failed: {total_failed}")
    print(f"  Still processing: {total_processing}")
    print(f"  All batches complete: {'Yes' if all_ended else 'No'}")
    
    return all_ended

def download_all_results(batch_ids, output_file="geolocation_results.jsonl"):
    """Download results from all batch chunks"""
    all_results = []
    
    print("Downloading results from all batches...\n")
    
    for batch_info in batch_ids:
        batch_id = batch_info['batch_id']
        chunk_num = batch_info['chunk_num']
        
        try:
            batch = client.beta.messages.batches.retrieve(batch_id)
            
            if batch.processing_status != "ended":
                print(f"Chunk {chunk_num} not ready yet. Status: {batch.processing_status}")
                continue
            
            # Get results for this batch
            results = client.beta.messages.batches.results(batch_id)
            
            chunk_results = list(results)
            all_results.extend(chunk_results)
            
            print(f"✅ Downloaded {len(chunk_results)} results from chunk {chunk_num}")
            
        except Exception as e:
            print(f"❌ Error downloading chunk {chunk_num}: {e}")
            continue
    
    if all_results:
        # Save all results to file
        with open(output_file, 'w') as f:
            for result in all_results:
                f.write(json.dumps(result) + '\n')
        
        print(f"\n✅ All results saved to {output_file}")
        print(f"Total results: {len(all_results)}")
        return True
    else:
        print("❌ No results to save")
        return False

def parse_results(results_file="geolocation_results.jsonl", mapping_file="filename_mapping.json"):
    """Parse results and extract coordinates, mapping back to original filenames"""
    coordinates = []
    
    # Load filename mapping
    try:
        with open(mapping_file, 'r') as f:
            filename_mapping = json.load(f)
    except:
        print("Warning: Could not load filename mapping. Using custom_id as filename.")
        filename_mapping = {}
    
    with open(results_file, 'r') as f:
        for line in f:
            try:
                result = json.loads(line)
                
                # Extract custom_id and map to original filename
                custom_id = result.get('custom_id', '')
                filename = filename_mapping.get(custom_id, custom_id)
                
                # Extract coordinates from response
                if result.get('result', {}).get('type') == 'message':
                    content = result['result']['content'][0]['text']
                    
                    coordinates.append({
                        'filename': filename,
                        'response': content,
                        'custom_id': custom_id
                    })
                    
            except Exception as e:
                print(f"Error parsing result: {e}")
                continue
    
    return coordinates

# Main execution
if __name__ == "__main__":
    # STEP 1: Set your image folder path
    IMAGE_FOLDER = "your-image-folder-path-here"
    
    print("Step 1: Creating batch requests...")
    requests = create_batch_requests(IMAGE_FOLDER)
    
    if not requests:
        print("No requests created. Check your image folder path.")
        exit()
    
    print(f"Created {len(requests)} requests")
    
    # Debug: Check custom_id lengths
    max_custom_id_length = max(len(req['custom_id']) for req in requests)
    print(f"Maximum custom_id length: {max_custom_id_length}")
    
    if max_custom_id_length > 64:
        print("❌ Warning: Some custom_ids are still too long!")
        for req in requests:
            if len(req['custom_id']) > 64:
                print(f"   Too long: {req['custom_id']} ({len(req['custom_id'])} chars)")
    else:
        print("✅ All custom_ids are within 64 character limit")
    
    # STEP 2: Submit batch in chunks
    print("\nStep 2: Submitting batches in chunks...")
    batch_ids = submit_batch_chunks(requests, chunk_size=50)  # Adjust chunk_size if needed
    
    if batch_ids:
        print(f"\n✅ All batches submitted! Created {len(batch_ids)} batch chunks")
        print("Save these Batch IDs:")
        
        # Save batch IDs to file for later reference
        with open("batch_ids.json", "w") as f:
            json.dump(batch_ids, f, indent=2)
        print("📁 Batch IDs saved to batch_ids.json")
        
        for i, batch_info in enumerate(batch_ids):
            print(f"  Chunk {batch_info['chunk_num']}: {batch_info['batch_id']}")
        
        # STEP 3: Check initial status
        print("\nStep 3: Checking initial status...")
        check_all_batches_status(batch_ids)
        
        print(f"\nYour batches are processing. To check status later, run:")
        print(f"with open('batch_ids.json', 'r') as f: batch_ids = json.load(f)")
        print(f"check_all_batches_status(batch_ids)")
        print(f"\nTo download results when ready:")
        print(f"download_all_results(batch_ids)")
        
    else:
        print("Failed to submit batches")

# Helper functions you can call separately:
# Load batch IDs: with open('batch_ids.json', 'r') as f: batch_ids = json.load(f)
# check_all_batches_status(batch_ids)
# download_all_results(batch_ids)
# coordinates = parse_results("geolocation_results.jsonl", "filename_mapping.json")