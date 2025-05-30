import json
from anthropic import Anthropic

# Initialize the Anthropic client
client = Anthropic(api_key="your-api-key-here")

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
                succeeded = getattr(counts, 'succeeded', 0)
                failed = getattr(counts, 'failed', 0)
                processing = getattr(counts, 'processing', 0)
                
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
                # Convert the result object to a dictionary
                result_dict = {
                    'custom_id': result.custom_id,
                    'result': {}
                }
                
                # Check if this is a successful result
                if hasattr(result, 'result') and hasattr(result.result, 'message'):
                    # Successful message result
                    message = result.result.message
                    result_dict['result'] = {
                        'type': 'message',
                        'content': []
                    }
                    
                    # Extract content from the message
                    if hasattr(message, 'content'):
                        for content_item in message.content:
                            if hasattr(content_item, 'text'):
                                result_dict['result']['content'].append({
                                    'type': 'text',
                                    'text': content_item.text
                                })
                elif hasattr(result, 'result') and hasattr(result.result, 'error'):
                    # Error result
                    error = result.result.error
                    result_dict['result'] = {
                        'type': 'error',
                        'error': {
                            'type': getattr(error, 'type', 'unknown'),
                            'message': getattr(error, 'message', str(error))
                        }
                    }
                else:
                    # Fallback - try to get any available data
                    result_dict['result'] = {
                        'type': 'unknown',
                        'raw_result': str(result.result) if hasattr(result, 'result') else 'no_result'
                    }
                
                f.write(json.dumps(result_dict) + '\n')
        
        print(f"\n✅ All results saved to {output_file}")
        print(f"Total results: {len(all_results)}")
        return True
    else:
        print("❌ No results to save")
        return False

def debug_results_structure(results_file="geolocation_results.jsonl", num_lines=3):
    """Debug function to inspect the structure of saved results"""
    print(f"Inspecting structure of {results_file}...")
    
    with open(results_file, 'r') as f:
        for i, line in enumerate(f):
            if i >= num_lines:
                break
                
            result = json.loads(line)
            print(f"\nResult {i+1}:")
            print(f"  Custom ID: {result.get('custom_id', 'N/A')}")
            print(f"  Result keys: {list(result.get('result', {}).keys())}")
            
            # Print the full result structure for debugging
            print(f"  Full result: {json.dumps(result, indent=2)}")

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
    
    print(f"Debug: Loading results from {results_file}")
    
    with open(results_file, 'r') as f:
        for line_num, line in enumerate(f, 1):
            try:
                result = json.loads(line)
                
                # Debug: Print structure of first result
                if line_num == 1:
                    print(f"Debug: First result structure: {list(result.keys())}")
                    if 'result' in result:
                        print(f"Debug: Result content keys: {list(result['result'].keys())}")
                
                # Extract custom_id and map to original filename
                custom_id = result.get('custom_id', '')
                filename = filename_mapping.get(custom_id, custom_id)
                
                # Extract coordinates from response - Updated structure
                if result.get('result', {}).get('type') == 'message':
                    content_list = result['result'].get('content', [])
                    if content_list and len(content_list) > 0:
                        # Get the text from the first content item
                        content_text = content_list[0].get('text', '')
                        
                        if content_text:
                            coordinates.append({
                                'filename': filename,
                                'response': content_text,
                                'custom_id': custom_id
                            })
                elif result.get('result', {}).get('type') == 'error':
                    # Handle error responses
                    error_info = result['result'].get('error', {})
                    print(f"Error in {filename}: {error_info.get('message', 'Unknown error')}")
                    
            except Exception as e:
                print(f"Error parsing line {line_num}: {e}")
                continue
    
    print(f"Debug: Successfully parsed {len(coordinates)} results")
    return coordinates

# Main execution
if __name__ == "__main__":
    # Load batch IDs
    try:
        with open('batch_ids.json', 'r') as f:
            batch_ids = json.load(f)
        print(f"Loaded {len(batch_ids)} batch chunks from batch_ids.json\n")
    except FileNotFoundError:
        print("❌ batch_ids.json not found. Make sure you're in the right directory.")
        exit()
    except Exception as e:
        print(f"❌ Error loading batch_ids.json: {e}")
        exit()
    
    # Check status
    print("=" * 50)
    print("CHECKING BATCH STATUS")
    print("=" * 50)
    all_complete = check_all_batches_status(batch_ids)
    
    if all_complete:
        print("\n🎉 All batches are complete!")
        
        # Ask if user wants to download results
        download = input("\nDownload results now? (y/n): ").lower().strip()
        if download in ['y', 'yes']:
            print("\n" + "=" * 50)
            print("DOWNLOADING RESULTS")
            print("=" * 50)
            success = download_all_results(batch_ids)
            
            if success:
                print("\n" + "=" * 50)
                print("PARSING RESULTS")
                print("=" * 50)
                coordinates = parse_results()
                print(f"\nParsed {len(coordinates)} results")
                
                # Show first few results as preview
                print("\nFirst 3 results:")
                for i, coord in enumerate(coordinates[:3]):
                    print(f"  {i+1}. {coord['filename']}")
                    print(f"     {coord['response']}")
                    print()
    else:
        print("\n⏳ Some batches are still processing. Run this script again later.")
    
    print("\nAvailable functions:")
    print("- check_all_batches_status(batch_ids)")
    print("- download_all_results(batch_ids)")
    print("- parse_results('geolocation_results.jsonl', 'filename_mapping.json')")
