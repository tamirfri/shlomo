from concurrent.futures import ThreadPoolExecutor as TPool #, ProcessPoolExecutor as PPool
from urllib.request import urlopen, pathname2url
from collections import defaultdict
from functools import partial
from shelve import open, DbfilenameShelf
from json import load

SHLOMO = "ARTZI SHLOMO"
FIELDS_IN_PAGE = 9
INNER_MAX_WORKERS = 2
OUTER_MAX_WORKERS = 2

def parse_title(name:str, title:str):
  title = title.translate(str.maketrans({',' : '/', '&' : '/', ')' : '('}))
  first2remove = title.find('(')
  if first2remove != -1:
    second = title.rfind('(')
    title = title[:first2remove] + title[second+1:]
  singers = tuple(map(str.strip, title.split('/')))
  if name not in singers:
    return frozenset()
  return frozenset(singer for singer in singers if singer and name not in singer)

def gen_parse_titles(name:str, answer:dict):
  singer_sets = map(partial(parse_title, name), (field['performerEngName'] for field in answer['pageResults']))
  return frozenset.union(*singer_sets)

def gen_get_request(name:str=SHLOMO, page:int=1):
  print(f"<< {page = },\t{name = } >>\n", end='', flush=True)
  url = f"https://nocs.acum.org.il/acumsitesearchdb/searchdb?primarySearchByTypeKey=3&primarySearchByTypeText={pathname2url(name)}&resultTypeKey=performer&pageNumber={page}&searchMethodTypeKey=partial&resultSortTypeKey=alphabetical&resultSortFieldKey=title"
  answer = load(urlopen(url))
  if answer['errorCode']:
    print(answer, end='', flush=True)
    return None # error occured or pages ended
  return answer['data']['resultTypeInfos'][0]

def gen_request_titles(name:str=SHLOMO, page:int=1):
  answer = gen_get_request(name, page)
  return gen_parse_titles(name, answer)

def get_first_request(name:str=SHLOMO):
  get_request = partial(gen_get_request, name)
  parse_titles = partial(gen_parse_titles, name)
  other_pages_call = partial(gen_request_titles, name)
  answer = get_request(1)
  count = answer['count']
  first_page_singers = parse_titles(answer)
  pages_left = (count-1) // FIELDS_IN_PAGE
  pages_range = range(2, 2+pages_left)
  return (first_page_singers, other_pages_call, pages_range)

def get_friends(cache:DbfilenameShelf=None, name:str=SHLOMO):
  if cache is not None:
    friends = cache.get(name)
    if friends is not None:
      return cache[name]
  response = get_first_request(name)
  first_page_singers = response[0]
  parallel_searcher = response[1]
  pages_range = response[2]
  if not pages_range: # only 1 page
    friends = first_page_singers
  else:
    with TPool(INNER_MAX_WORKERS) as pool:
      next_pages_singers = pool.map(parallel_searcher, pages_range)
    friends = first_page_singers.union(*next_pages_singers)
  if cache is not None:
    cache[name] = friends
    cache.sync()
  return friends

if __name__ == '__main__':
  shlomo_numbers = {"ARTZI SHLOMO": 0, "ARTSI SHLOMO" : 0}
  next_layer = shlomo_numbers
  with open('cache') as cache:
    friends = partial(get_friends, cache)
    for i in range(1, 4):
      with TPool(OUTER_MAX_WORKERS) as pool:
        singers = pool.map(friends, next_layer.keys())
      next_layer = {singer:i for singer in frozenset.union(*singers) if singer not in shlomo_numbers}
      shlomo_numbers.update(next_layer)
  print("finished!")
  results = defaultdict(list)
  for name, i in shlomo_numbers.items():
    results[i].append(name)
  for i, lst in results.items():
    print(f"\t- {i} - shlomo distance: {sorted(lst)}")
