from typing import Dict, List, Optional
import json
import asyncio
import re
import math
from collections import Counter
from openai import AsyncOpenAI
from configs.config import Config

class BM25:
    def __init__(self, k1=1.5, b=0.75):
        self.k1 = k1
        self.b = b
    
    def tokenize(self, text: str) -> List[str]:
        return [word.lower() for word in re.findall(r'\b\w+\b', text) if len(word) > 2]
    
    def compute_idf(self, documents: List[List[str]]) -> Dict[str, float]:
        N = len(documents)
        idf = {}
        all_words = set(word for doc in documents for word in doc)
        for word in all_words:
            containing_docs = sum(1 for doc in documents if word in doc)
            idf[word] = math.log((N - containing_docs + 0.5) / (containing_docs + 0.5))
        return idf
    
    def score(self, query_tokens: List[str], document_tokens: List[str], 
              avg_doc_len: float, idf: Dict[str, float]) -> float:
        score = 0.0
        doc_len = len(document_tokens)
        doc_term_freq = Counter(document_tokens)
        for term in query_tokens:
            if term in doc_term_freq:
                tf = doc_term_freq[term]
                term_idf = idf.get(term, 0)
                numerator = tf * (self.k1 + 1)
                denominator = tf + self.k1 * (1 - self.b + self.b * (doc_len / avg_doc_len))
                score += term_idf * (numerator / denominator)
        return score

class TaggingService:
    def __init__(self, config: Config):
        self.config = config
        self.openai_client = AsyncOpenAI(api_key=config.openai_api_key)
        self.abs_tags = self._load_tag_files()
        self.parsed_abs_tags = self._parse_abstractive_tags()
        self.product_tags = self._load_extractive_tags('product_tags.json')
        self.indication_tags = self._load_extractive_tags('indication_tags.json')
        self.bm25 = BM25()
        self._prepare_product_bm25_data()
        self._prepare_indication_bm25_data()
    
    def _load_tag_files(self):
        try:
            with open('abs_tags.json', 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError) as e:
            raise Exception(f"Tag file error: {e}")
    
    def _load_extractive_tags(self, filename: str) -> Dict:
        try:
            with open(filename, 'r') as f:
                return json.load(f)
        except (FileNotFoundError, json.JSONDecodeError):
            return {}
    
    def _prepare_product_bm25_data(self):
        self.product_documents = []
        self.product_to_tokens = {}
        for product_name, synonyms in self.product_tags.items():
            if isinstance(synonyms, list):
                product_text = " ".join([product_name] + synonyms)
                product_tokens = self.bm25.tokenize(product_text)
                self.product_documents.append(product_tokens)
                self.product_to_tokens[product_name] = product_tokens
        
        if self.product_documents:
            self.product_idf = self.bm25.compute_idf(self.product_documents)
            self.avg_product_doc_len = sum(len(doc) for doc in self.product_documents) / len(self.product_documents)
        else:
            self.product_idf = {}
            self.avg_product_doc_len = 0
    
    def _prepare_indication_bm25_data(self):
        self.indication_documents = []
        self.category_to_tokens = {}
        for category_name, category_data in self.indication_tags.items():
            if isinstance(category_data, dict):
                category_text_parts = [category_name]
                for indication_name, indication_data in category_data.items():
                    if isinstance(indication_data, dict):
                        category_text_parts.append(indication_name)
                        synonyms = indication_data.get('Synonyms', [])
                        category_text_parts.extend([s for s in synonyms if isinstance(s, str)])
                        for sub_key, sub_value in indication_data.items():
                            if sub_key != 'Synonyms' and isinstance(sub_value, dict):
                                category_text_parts.append(sub_key)
                                sub_synonyms = sub_value.get('Synonyms', [])
                                category_text_parts.extend([s for s in sub_synonyms if isinstance(s, str)])
                
                category_tokens = self.bm25.tokenize(" ".join(category_text_parts))
                self.indication_documents.append(category_tokens)
                self.category_to_tokens[category_name] = category_tokens
        
        if self.indication_documents:
            self.indication_idf = self.bm25.compute_idf(self.indication_documents)
            self.avg_indication_doc_len = sum(len(doc) for doc in self.indication_documents) / len(self.indication_documents)
        else:
            self.indication_idf = {}
            self.avg_indication_doc_len = 0
    
    def _extract_tags_with_bm25(self, text: str, token_mapping: Dict, idf: Dict, avg_doc_len: float, 
                               min_score_threshold: float = 1.0) -> Dict[str, float]:
        if not token_mapping:
            return {}
        
        text_tokens = self.bm25.tokenize(text)
        if not text_tokens:
            return {}
        
        scores = {}
        for name, tokens in token_mapping.items():
            score = self.bm25.score(text_tokens, tokens, avg_doc_len, idf)
            if score > 0:
                scores[name] = score
        
        if not scores:
            return {}
        
        total_score = sum(scores.values())
        normalized_scores = {name: (score / total_score) * 100 for name, score in scores.items()}
        filtered_scores = {name: round(score, 1) for name, score in normalized_scores.items() 
                          if score >= min_score_threshold}
        
        if filtered_scores:
            remaining_total = sum(filtered_scores.values())
            if remaining_total > 0:
                return {name: round((score / remaining_total) * 100, 1) 
                       for name, score in filtered_scores.items()}
        return {}
    
    def calculate_extractive_tags(self, text: str, min_score_threshold: float = 1.0) -> Dict:
        product_matches = self._extract_tags_with_bm25(text, self.product_to_tokens, 
                                                      self.product_idf, self.avg_product_doc_len, 
                                                      min_score_threshold)
        indication_matches = self._extract_tags_with_bm25(text, self.category_to_tokens, 
                                                         self.indication_idf, self.avg_indication_doc_len, 
                                                         min_score_threshold)
        
        return {"product": product_matches, "indication": indication_matches}
    
    def _parse_abstractive_tags(self):
        parsed = {}
        data_to_parse = self.abs_tags.get('Content Taxonomy', self.abs_tags)
        
        for main_category, category_data in data_to_parse.items():
            if not isinstance(category_data, dict):
                continue
            
            category_key = main_category.lower()
            if category_key == 'audience':
                category_key = 'audience'
            elif 'purpose' in category_key:
                category_key = 'content purpose'
            elif 'complexity' in category_key:
                category_key = 'content complexity'
            elif 'non clinical' in category_key or 'nonclinical' in category_key:
                category_key = 'non clinical topics'
            elif 'clinical topic' in category_key or 'clinical' in category_key:
                category_key = 'clinical topic'
            
            parsed[category_key] = {
                'definition': category_data.get('definition', ''),
                'subtags': {}
            }
            
            for key, value in category_data.items():
                if key in ['definition', 'synonyms']:
                    continue
                if isinstance(value, dict) and 'definition' in value:
                    parsed[category_key]['subtags'][key] = {
                        'definition': value.get('definition', ''),
                        'synonyms': value.get('synonyms', []),
                        'nested_subtags': {}
                    }
                    
                    for nested_key, nested_value in value.items():
                        if nested_key in ['definition', 'synonyms']:
                            continue
                        if isinstance(nested_value, dict) and 'definition' in nested_value:
                            parsed[category_key]['subtags'][key]['nested_subtags'][nested_key] = {
                                'definition': nested_value.get('definition', ''),
                                'synonyms': nested_value.get('synonyms', [])
                            }
        return parsed
    
    def prepare_categories_info(self):
        info = ["=== ABSTRACTIVE CATEGORIES ==="]
        for category_name, category_info in self.parsed_abs_tags.items():
            info.append(f"\n{category_name.upper()}:")
            info.append(f"Definition: {category_info['definition']}")
            if category_info['subtags']:
                info.append("Available subtags:")
                for subtag_name, subtag_info in category_info['subtags'].items():
                    synonyms_str = ', '.join(subtag_info['synonyms'][:3]) if subtag_info['synonyms'] else ""
                    synonym_part = f" (Synonyms: {synonyms_str})" if synonyms_str else ""
                    info.append(f"  - {subtag_name}: {subtag_info['definition']}{synonym_part}")
                    
                    if subtag_info.get('nested_subtags'):
                        for nested_name, nested_info in subtag_info['nested_subtags'].items():
                            nested_synonyms = ', '.join(nested_info['synonyms'][:2]) if nested_info['synonyms'] else ""
                            nested_synonym_part = f" (Synonyms: {nested_synonyms})" if nested_synonyms else ""
                            info.append(f"    * {nested_name}: {nested_info['definition']}{nested_synonym_part}")
        return '\n'.join(info)
    
    def chunk_text(self, text: str, chunk_size: int = 5000) -> List[str]:
        words = text.split()
        chunks = []
        current_chunk = []
        current_length = 0
        
        for word in words:
            if current_length + len(word) > chunk_size and current_chunk:
                chunks.append(' '.join(current_chunk))
                current_chunk = [word]
                current_length = len(word)
            else:
                current_chunk.append(word)
                current_length += len(word) + 1
        
        if current_chunk:
            chunks.append(' '.join(current_chunk))
        return chunks
    
    async def tag_chunk(self, chunk: str) -> Dict:
        categories_info = self.prepare_categories_info()
        system_prompt_template = """You are an expert content analyzer. Analyze the provided text content and identify relevant abstractive tags from the given categories. You must select ONLY the specific subtags provided in the available options, not generic terms.

Available Categories and Subtags:
{categories_info}

Important Rules:
1. ONLY use the exact subtag names provided in the categories above
2. You can also use synonyms of the subtags if they appear in the content
3. Multiple subtags can be selected from each category if relevant
4. Confidence scores should be between 0.0 and 1.0
5. Only select subtags you are highly confident about (>= 0.3)
6. If no specific subtags are relevant, return empty objects for that category
7. Look for exact matches, synonym matches, and conceptual matches based on definitions
8. Pay attention to the context and meaning of the content when selecting tags
9. For parent tags with nested subtags (like Treatment), identify both the parent and relevant nested subtags
10. Score nested subtags separately from their parent tags

Output Format:
Return ONLY a valid JSON object with this exact structure:
{{
    "abstractive": {{
        "audience": {{"exact_subtag_name_or_synonym": confidence_score}},
        "content purpose": {{"exact_subtag_name_or_synonym": confidence_score}},
        "content complexity": {{"exact_subtag_name_or_synonym": confidence_score}},
        "non clinical topics": {{"exact_subtag_name_or_synonym": confidence_score}},
        "clinical topic": {{"exact_subtag_name_or_synonym": confidence_score}}
    }}
}}

Text Content to Analyze:
{content}"""
        
        prompt = system_prompt_template.format(categories_info=categories_info, content=chunk)
        
        models_to_try = ["gpt-4o"]
        for model in models_to_try:
            try:
                response = await self.openai_client.chat.completions.create(
                    model=model,
                    messages=[
                        {"role": "system", "content": "You are an expert content analyzer. Return only valid JSON responses with exact subtag names from the provided options."},
                        {"role": "user", "content": prompt}
                    ],
                    temperature=0.1,
                    max_tokens=800,
                    response_format={"type": "json_object"}
                )
                content = response.choices[0].message.content.strip()
                try:
                    result = json.loads(content)
                    validated_result = self._validate_and_clean_result(result)
                    return validated_result
                except json.JSONDecodeError:
                    continue
            except Exception as e:
                if "invalid_api_key" in str(e).lower():
                    raise Exception(f"Invalid API key. Please check your OpenAI API key.")
                elif "model_not_found" in str(e).lower() or "does not exist" in str(e).lower():
                    continue
                elif model == models_to_try[-1]:
                    raise Exception(f"All models failed. Last error: {e}")
                else:
                    continue
        return self._get_empty_result()
    
    def _get_empty_result(self):
        return {
            "abstractive": {
                "audience": {},
                "content purpose": {},
                "content complexity": {},
                "non clinical topics": {},
                "clinical topic": {}
            }
        }
    
    def _validate_and_clean_result(self, result: Dict) -> Dict:
        cleaned = self._get_empty_result()
        if not isinstance(result, dict):
            return cleaned
        
        if "abstractive" in result and isinstance(result["abstractive"], dict):
            category_mapping = {
                "audience": "audience",
                "content purpose": "content purpose", 
                "content complexity": "content complexity",
                "non clinical topics": "non clinical topics",
                "clinical topic": "clinical topic"
            }
            
            for result_key, clean_key in category_mapping.items():
                if result_key in result["abstractive"] and isinstance(result["abstractive"][result_key], dict):
                    valid_subtags = {}
                    valid_nested_subtags = {}
                    synonym_to_main_tag = {}
                    synonym_to_nested_tag = {}
                    
                    if clean_key in self.parsed_abs_tags:
                        category_info = self.parsed_abs_tags[clean_key]
                        for subtag_name, subtag_info in category_info['subtags'].items():
                            valid_subtags[subtag_name] = subtag_info
                            for synonym in subtag_info.get('synonyms', []):
                                synonym_to_main_tag[synonym.lower()] = subtag_name
                            
                            for nested_name, nested_info in subtag_info.get('nested_subtags', {}).items():
                                valid_nested_subtags[nested_name] = nested_info
                                for synonym in nested_info.get('synonyms', []):
                                    synonym_to_nested_tag[synonym.lower()] = nested_name
                    
                    for tag, score in result["abstractive"][result_key].items():
                        if isinstance(score, (int, float)) and 0 <= score <= 1:
                            main_tag_name = None
                            
                            if tag in valid_subtags:
                                main_tag_name = tag
                            elif tag.lower() in synonym_to_main_tag:
                                main_tag_name = synonym_to_main_tag[tag.lower()]
                            elif tag in valid_nested_subtags:
                                main_tag_name = tag
                            elif tag.lower() in synonym_to_nested_tag:
                                main_tag_name = synonym_to_nested_tag[tag.lower()]
                            else:
                                for valid_tag in list(valid_subtags.keys()) + list(valid_nested_subtags.keys()):
                                    if tag.lower() == valid_tag.lower():
                                        main_tag_name = valid_tag
                                        break
                            
                            if main_tag_name:
                                cleaned["abstractive"][clean_key][main_tag_name] = float(score)
        
        return cleaned
    
    def _create_hierarchical_clinical_topic_structure(self, clinical_tags: Dict[str, float]) -> Dict:
        if not clinical_tags:
            return {}
        
        hierarchical_structure = {}
        parent_subtag_mapping = {}
        subtag_parent_mapping = {}
        
        if 'clinical topic' in self.parsed_abs_tags:
            for parent_tag, parent_info in self.parsed_abs_tags['clinical topic']['subtags'].items():
                subtag_parent_mapping[parent_tag] = []
                for nested_tag in parent_info.get('nested_subtags', {}):
                    parent_subtag_mapping[nested_tag] = parent_tag
                    subtag_parent_mapping[parent_tag].append(nested_tag)
        
        parent_groups = {}
        standalone_tags = {}
        
        for tag, score in clinical_tags.items():
            if tag in parent_subtag_mapping:
                parent = parent_subtag_mapping[tag]
                if parent not in parent_groups:
                    parent_groups[parent] = {'subtags': {}, 'parent_score': 0, 'has_parent_detected': False}
                parent_groups[parent]['subtags'][tag] = score
            else:
                if tag in subtag_parent_mapping:
                    if tag not in parent_groups:
                        parent_groups[tag] = {'subtags': {}, 'parent_score': score, 'has_parent_detected': True}
                    else:
                        parent_groups[tag]['parent_score'] = score
                        parent_groups[tag]['has_parent_detected'] = True
                else:
                    standalone_tags[tag] = score
        
        for parent_tag, group_info in parent_groups.items():
            subtags_dict = group_info['subtags']
            parent_score = group_info['parent_score']
            has_parent_detected = group_info['has_parent_detected']
            
            if has_parent_detected and subtags_dict:
                hierarchical_structure[parent_tag] = {
                    "score": parent_score,
                    "subtags": subtags_dict
                }
            elif has_parent_detected and not subtags_dict:
                hierarchical_structure[parent_tag] = {"score": parent_score, "subtags": {}}
            elif not has_parent_detected and subtags_dict:
                for subtag, score in subtags_dict.items():
                    parent = parent_subtag_mapping.get(subtag)
                    if parent:
                        if parent not in hierarchical_structure:
                            hierarchical_structure[parent] = {"score": 0, "subtags": {}}
                        hierarchical_structure[parent]["subtags"][subtag] = score
                    else:
                        hierarchical_structure[subtag] = {"score": score, "subtags": {}}
        
        for tag, score in standalone_tags.items():
            hierarchical_structure[tag] = {"score": score, "subtags": {}}
        
        return hierarchical_structure
    
    def _renormalize_hierarchical_clinical_tags(self, hierarchical_tags: Dict) -> Dict:
        if not hierarchical_tags:
            return hierarchical_tags
        
        total_parent_score = sum(
            tag_info["score"] if isinstance(tag_info, dict) and "score" in tag_info else tag_info
            for tag_info in hierarchical_tags.values()
        )
        
        if total_parent_score == 0:
            return hierarchical_tags
        
        renormalized_tags = {}
        for tag, tag_info in hierarchical_tags.items():
            if isinstance(tag_info, dict) and "score" in tag_info:
                old_score = tag_info["score"]
                new_score = round((old_score / total_parent_score) * 100, 1)
                renormalized_tags[tag] = {"score": new_score, "subtags": tag_info.get("subtags", {})}
            else:
                if isinstance(tag_info, (int, float)):
                    new_score = round((tag_info / total_parent_score) * 100, 1)
                    renormalized_tags[tag] = new_score
                else:
                    renormalized_tags[tag] = tag_info
        
        return renormalized_tags
    
    def _calculate_clinical_nonclinical_distribution(self, chunk_results: List[Dict]) -> Dict[str, float]:
        clinical_raw_scores = []
        non_clinical_raw_scores = []
        
        for result in chunk_results:
            if not isinstance(result, dict) or "abstractive" not in result:
                continue
                
            clinical_topics = result["abstractive"].get("clinical topic", {})
            for tag, score in clinical_topics.items():
                if isinstance(score, (int, float)) and score >= 0.3:
                    clinical_raw_scores.append(score)
            
            non_clinical_topics = result["abstractive"].get("non clinical topics", {})
            for tag, score in non_clinical_topics.items():
                if isinstance(score, (int, float)) and score >= 0.3:
                    non_clinical_raw_scores.append(score)
        
        clinical_avg_confidence = sum(clinical_raw_scores) / len(clinical_raw_scores) if clinical_raw_scores else 0.0
        non_clinical_avg_confidence = sum(non_clinical_raw_scores) / len(non_clinical_raw_scores) if non_clinical_raw_scores else 0.0
        
        total_chunks = len(chunk_results)
        clinical_frequency = len(set(i for i, result in enumerate(chunk_results) 
                                   if result.get("abstractive", {}).get("clinical topic", {}))) / total_chunks if total_chunks > 0 else 0
        non_clinical_frequency = len(set(i for i, result in enumerate(chunk_results) 
                                       if result.get("abstractive", {}).get("non clinical topics", {}))) / total_chunks if total_chunks > 0 else 0
        
        clinical_weighted = clinical_avg_confidence * clinical_frequency
        non_clinical_weighted = non_clinical_avg_confidence * non_clinical_frequency
        
        total_weighted = clinical_weighted + non_clinical_weighted
        
        if total_weighted == 0:
            return {
                "clinical": 0.0,
                "non_clinical": 0.0
            }
        
        clinical_percentage = round((clinical_weighted / total_weighted) * 100, 1)
        non_clinical_percentage = round((non_clinical_weighted / total_weighted) * 100, 1)
        
        if clinical_percentage + non_clinical_percentage != 100.0:
            diff = 100.0 - (clinical_percentage + non_clinical_percentage)
            if abs(diff) <= 0.1:
                if clinical_percentage >= non_clinical_percentage:
                    clinical_percentage += diff
                else:
                    non_clinical_percentage += diff
            clinical_percentage = max(0.0, round(clinical_percentage, 1))
            non_clinical_percentage = max(0.0, round(non_clinical_percentage, 1))
        
        return {
            "clinical": clinical_percentage,
            "non_clinical": non_clinical_percentage
        }
    
    def combine_chunk_results(self, chunk_results: List[Dict]) -> Dict:
        combined = self._get_empty_result()
        abstractive_occurrences = {
            "audience": {},
            "content purpose": {},
            "content complexity": {},
            "non clinical topics": {},
            "clinical topic": {}
        }
        
        for result in chunk_results:
            if not isinstance(result, dict) or "abstractive" not in result:
                continue
            for category in abstractive_occurrences.keys():
                if category in result["abstractive"]:
                    for tag, score in result["abstractive"][category].items():
                        if score >= 0.3:
                            if tag not in abstractive_occurrences[category]:
                                abstractive_occurrences[category][tag] = []
                            abstractive_occurrences[category][tag].append(score)
        
        for category in abstractive_occurrences.keys():
            if abstractive_occurrences[category]:
                category_tags = {}
                total_chunks = len(chunk_results)
                
                for tag, scores in abstractive_occurrences[category].items():
                    avg_score = sum(scores) / len(scores)
                    frequency_weight = len(scores) / total_chunks
                    raw_weighted_score = avg_score * frequency_weight
                    category_tags[tag] = raw_weighted_score
                
                if category_tags:
                    total_raw_score = sum(category_tags.values())
                    if total_raw_score > 0:
                        for tag in category_tags:
                            normalized_score = (category_tags[tag] / total_raw_score) * 100
                            category_tags[tag] = round(normalized_score, 1)
                
                if category == "clinical topic":
                    hierarchical_tags = self._create_hierarchical_clinical_topic_structure(category_tags)
                    hierarchical_tags = self._renormalize_hierarchical_clinical_tags(hierarchical_tags)
                    combined["abstractive"][category] = hierarchical_tags
                else:
                    combined["abstractive"][category] = category_tags
        
        return combined
    
    async def tag_document(self, text: str, chunk_size: int = 5000, min_extractive_threshold: float = 1.0) -> Dict:
        if not text.strip():
            return {
                "extractive": {"indication": {}, "product": {}},
                "abstractive": {"audience": {}, "content purpose": {}, "content complexity": {}, 
                               "non clinical topics": {}, "clinical topic": {}},
                "content_distribution": {"clinical": 0.0, "non_clinical": 0.0}
            }
        
        chunks = self.chunk_text(text, chunk_size)
        chunk_results = []
        batch_size = 2
        
        for i in range(0, len(chunks), batch_size):
            batch = chunks[i:i+batch_size]
            batch_tasks = [self.tag_chunk(chunk) for chunk in batch]
            batch_results = await asyncio.gather(*batch_tasks)
            chunk_results.extend(batch_results)
            if i + batch_size < len(chunks):
                await asyncio.sleep(2)
        
        content_distribution = self._calculate_clinical_nonclinical_distribution(chunk_results)
        abstractive_result = self.combine_chunk_results(chunk_results)
        extractive_result = self.calculate_extractive_tags(text, min_extractive_threshold)
        
        return {
            "extractive": {"indication": extractive_result["indication"], "product": extractive_result["product"]},
            "abstractive": abstractive_result["abstractive"],
            "content_distribution": content_distribution
        }