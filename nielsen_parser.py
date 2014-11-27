
import os
import zipfile
import shutil
import pprint
import subprocess
import time
import resources
from ftplib import FTP
from datetime import date, timedelta


base_directory =  os.path.dirname(os.path.realpath(__file__))
input_folder = 'input_data'
input_folder_path = os.path.join(base_directory, input_folder)
output_folder = 'output_data'
output_folder_path = os.path.join(base_directory, output_folder)
archived_folder = 'archived_data'
archived_folder_path = os.path.join(base_directory, archived_folder)
server_name = resources.server_name
user = resources.user
passwd = resources.passwd

wrong_date = []
album_rows = []
track_rows = []
output_file_names = []


def is_valid_date(date):
	#mm-dd-yyyy
	try:
		date = date.split('/')
		if (int(date[0]) > 0 and int(date[0]) <= 12) and (int(date[1]) > 0 and int(date[1]) <= 31) and (int(date[2])>= 1900 and int(date[2]) <= 2300):
			return '/'.join(date)
		else:
			return ''
	except:
		wrong_date.append(date)
		return ''

def find_lastest(list_of_files):
	
	week = []
	for f in list_of_files:
		week.append(int(f[:-4][-4:]))

	latest_week = max(week)

	for f in list_of_files:
		if str(latest_week) in f:
			lastest_report_week = f
			break
	return lastest_report_week


def connect_to_sever(server_name, user, passwd):

	ftp = FTP(server_name)
	ftp.login(user, passwd)
	return ftp

def fetch_data(filematch = 'amazon_sales_report*'):
	
	ftp = connect_to_sever(server_name, user, passwd)
	files = ftp.nlst(filematch)
	lastest_report_week = find_lastest(files)

	with open(lastest_report_week, 'wb') as f :
		ftp.retrbinary("RETR " + lastest_report_week, f.write)
		print('>>>lastest sales report downloaded')
	
	ftp.close()
	latest_week = lastest_report_week[:-4][-4:]
	return lastest_report_week
	
def unzip_report(ziped_file):
	
	with zipfile.ZipFile(ziped_file) as zfile:
		for name in zfile.namelist():
			if name.endswith('txt'):
				source = zfile.open(name)
				target_file = name.split('/')[-1]
				target = file(input_folder_path + '/'+ target_file, "wb")
				with source, target:
					shutil.copyfileobj(source, target)
	os.remove(ziped_file)
	print('>>>Files unziped to {0} folder'.format(input_folder))

def parse_albums(line):
		
	SSNUMBER = line[0:13].strip() #13
	ARTIST = line[13:43].strip().replace('*', ' ').encode('string-escape')  #30
	TITLE = line[43:73].strip().encode('string-escape')   #30
	RELEASE_DATE = is_valid_date(line[73:84].strip()) #11
	WEEK_ENDING = is_valid_date(line[84:95].strip())  #11
	AMAZON_SALES_PHYSICAL = line[95:104].strip() #9 
	AMAZON_SALES_DIGITAL = line[104:112].strip() #8
	SOUNDSCAN_SALES_PHYSICAL = line[112:120].strip() #8 
	SOUNDSCAN_SALES_DIGITAL = line[120:128].strip() #8
	CORE_GENRE = line[128:144].strip() #16
	AGE = line[144:159].strip() # 15
		
	row = {
				'SSNUMBER': SSNUMBER ,
				'ARTIST' : ARTIST,
				'TITLE' : TITLE,
				'RELEASE_DATE' : RELEASE_DATE,
				'WEEK_ENDING': WEEK_ENDING,
				'AMAZON_SALES_DIGITAL': AMAZON_SALES_DIGITAL,
				'AMAZON_SALES_PHYSICAL' : AMAZON_SALES_PHYSICAL,
				'SOUNDSCAN_SALES_DIGITAL' : SOUNDSCAN_SALES_DIGITAL,
				'SOUNDSCAN_SALES_PHYSICAL' : SOUNDSCAN_SALES_PHYSICAL,
				'CORE_GENRE' : CORE_GENRE,
				'AGE':  AGE
		}

	return row, (SSNUMBER, ARTIST, TITLE, RELEASE_DATE, WEEK_ENDING, AMAZON_SALES_DIGITAL, AMAZON_SALES_PHYSICAL, SOUNDSCAN_SALES_DIGITAL, SOUNDSCAN_SALES_PHYSICAL, CORE_GENRE, AGE )

def parse_tracks(line):

	ISRC = line[0:13].strip() #13
	ARTIST = line[13:43].strip().replace('*', ' ').encode('string-escape')  #30
	TITLE = line[43:73].strip().encode('string-escape')   #30
	RELEASE_DATE = is_valid_date(line[73:84].strip()) #11
	WEEK_ENDING = is_valid_date(line[84:95].strip()) #11
	AMAZON_SALES_DIGITAL = line[95:104].strip() #9
	SOUNDSCAN_SALES_DIGITAL = line[104:112].strip() #8 
	CORE_GENRE = line[112:128].strip() #16
	AGE = line[128:].strip() # 15
		
	row = {
				'ISRC': ISRC ,
				'ARTIST' : ARTIST,
				'TITLE' : TITLE,
				'RELEASE_DATE' : RELEASE_DATE,
				'WEEK_ENDING': WEEK_ENDING,
				'AMAZON_SALES_DIGITAL': AMAZON_SALES_DIGITAL,
				'SOUNDSCAN_SALES_DIGITAL' : SOUNDSCAN_SALES_DIGITAL,
				'CORE_GENRE' : CORE_GENRE,
				'AGE':  AGE
		}

	return row, (ISRC, ARTIST, TITLE, RELEASE_DATE, WEEK_ENDING, AMAZON_SALES_DIGITAL, SOUNDSCAN_SALES_DIGITAL, CORE_GENRE, AGE )

def parse_albums_ref(line):

	AMAZON_UPC, SSNUMBER = line[:16], line[16:].strip()
	return (AMAZON_UPC, SSNUMBER)


def process_albums(input_file_name):

	list_of_rows = []

	with open(input_file_name) as f:
		album_header = f.readline().split()		
		for line in f:
			row, row_1 = parse_albums(line)
			album_rows.append(row)
			list_of_rows.append(row_1)

	this_week = input_file_name[:-4][-4:]
	output_file_name = os.path.join(output_folder_path ,'albums_{0}.tsv'.format(this_week))
	output_file_names.append(output_file_name)
	with open(output_file_name, 'w') as fout:
		for item in list_of_rows:		
			fout.writelines('\t'.join(item) + '\t' + '1' + '\n')
	print('>>>Albums data is ready in the output_data folder')

def process_xref_albums(input_file_name ):

	list_of_upcs = []
	with open(input_file_name) as f:
		header = f.readline().split()		
		for line in f:		
			row = parse_albums_ref(line)
			list_of_upcs.append(row)

	this_week = input_file_name[:-9][-4:]
	output_file_name = os.path.join(output_folder_path ,'albums__xref_{0}.tsv'.format(this_week))
	output_file_names.append(output_file_name)
	with open(output_file_name, 'w') as fout:
		for item in list_of_upcs:
			fout.writelines('\t'.join(item) + '\t' + '1'  + '\t' + '1' + '\n')
	print('>>>Albums ref data is ready in the output_data folder')

def process_tracks(input_file_name):

	list_of_rows = []
	
	with open(input_file_name) as f:
		header = f.readline()	
		for line in f:
			row, row_1 = parse_tracks(line)
			track_rows.append(row)
			list_of_rows.append(row_1)

	this_week = input_file_name[:-4][-4:]
	output_file_name = os.path.join(output_folder_path ,'tracks_{0}.tsv'.format(this_week))
	output_file_names.append(output_file_name)
	with open(output_file_name, 'w') as fout:	
		for item in list_of_rows:		
			fout.writelines('\t'.join(item) + '\t' + '1' + '\n')
	print('>>>Tracks data is ready in the output_data folder')

def parse_files(): 

	archived_files = [ files for files in os.listdir(archived_folder_path) if files.endswith('.txt')]
	input_files = [ files for files in os.listdir(input_folder_path) if files.endswith('.txt')]
	
	for f in input_files:
		if f not in archived_files:
			if 'XREF' in f:
				process_xref_albums(os.path.join(input_folder_path,f))
				shutil.move(os.path.join(input_folder_path,f), archived_folder_path)
			elif 'Track' in f:
				process_tracks(os.path.join(input_folder_path, f))
				shutil.move(os.path.join(input_folder_path,f), archived_folder_path)
			else:
				process_albums(os.path.join(input_folder_path,f))
				shutil.move(os.path.join(input_folder_path,f), archived_folder_path)
		else:
			print('>>>There is no new files to parse yet')


def summarize(list_of_rows):

	rd= {}
	we = {}
	age = {}
	genre= {}
	amz_digital = 0
	soundscan_digital = 0
	amz_physical = 0
	soundscan_physical = 0 

	for row in list_of_rows:
		rd[row['RELEASE_DATE']] = rd.get(row['RELEASE_DATE'], 0) + 1
		we[row['WEEK_ENDING']] = we.get(row['WEEK_ENDING'], 0) + 1
		age[row['AGE']] = age.get(row['AGE'], 0) + 1
		genre[row['CORE_GENRE']] = genre.get(row['CORE_GENRE'], 0) + 1
		amz_digital = amz_digital + int(row['AMAZON_SALES_DIGITAL'])
		soundscan_digital = soundscan_digital + int(row['SOUNDSCAN_SALES_DIGITAL'])
		amz_physical = int(row.get('AMAZON_SALES_PHYSICAL', 0 )) + amz_physical
		soundscan_physical = int(row.get('SOUNDSCAN_SALES_PHYSICAL', 0 )) + soundscan_physical
		
	return rd, we, age, genre, amz_digital, soundscan_digital, amz_physical, soundscan_physical


def test_tracks(list_of_tracks):

	rd, we, age, genre, amz_digital, soundscan_digital, _, _= summarize(list_of_tracks)

	print('>>>Tracks release date summary:')
	pprint.pprint(rd)
	print('>>>Tracks Week ending summary:')
	pprint.pprint(we)
	print('>>>Tracks age summary:')
	pprint.pprint(age)
	print('>>>Tracks genre summary:')
	pprint.pprint(genre)
	print('>>>amazon digital track sales:')
	print(amz_digital)
	print('>>>soundscan digital track sales:')
	print(soundscan_digital)

def test_albums(list_of_albums):

	rd, we,age,	genre, amz_digital, soundscan_digital, amz_physical, soundscan_physical = summarize(list_of_albums)

	print('>>>Albums release date summary:')
	pprint.pprint(rd)
	print('>>>Albums Week ending summary:')
	pprint.pprint(we)
	print('>>>Albums age summary:')
	pprint.pprint(age)
	print('>>>Albums genre summary:')
	pprint.pprint(genre)
	print('>>>Amazon digital album sales:')
	print(amz_digital)
	print('>>>soundscan digital album sales:')
	print(soundscan_digital)
	print('>>>Amazon amz_physical album sales:')
	print(amz_physical)
	print('>>>soundscan digital album sales:')
	print(soundscan_physical)

def upload_data():

	album_feed_name = 'NIELSEN_ALBUM_1'
	track_feed_name = 'NIELLSEN_TRACKS'
	album_ref_feed_name = 'NIELSE_ALBUM_MAP' 
	token = resources.token
	batchDate = str(date.today() + timedelta(days= -1)).replace('-', '/')
	
	
	upload_string = '''
					/apollo/env/DCSUploadClient/bin/dcs 
					-uploadFile {upFile} 
					-batchDate {batchDate} 
					-feedName {feedname} 
					-token {token}
					-partitionKey MARKETPLACE_ID 
					-partitionValues 1
					-maxRejects 100
					'''
	
	for file_name in output_file_names:
		if 'xref' in file_name:
                        
			#print(upload_string.format(upFile= file_name, batchDate=batchDate, feedname=album_ref_feed_name, token=token))
			upload_list_comd = upload_string.format(upFile= file_name, batchDate=batchDate, feedname=album_ref_feed_name, token=token).split()
			t = subprocess.check_output(upload_list_comd)
			print(t)
			time.sleep(40)
		elif 'tracks' in file_name:
			#print(upload_string.format(upFile= file_name, batchDate=batchDate, feedname=track_feed_name, token=token))
			upload_list_comd = upload_string.format(upFile= file_name, batchDate=batchDate, feedname=track_feed_name, token=token).split()
			t = subprocess.check_output(upload_list_comd)
			print(t)
			time.sleep(40)
		else:
			#print(upload_string.format(upFile= file_name, batchDate=batchDate, feedname=album_feed_name, token=token))
			upload_list_comd = upload_string.format(upFile= file_name, batchDate=batchDate, feedname=album_feed_name, token=token).split()
			t = subprocess.check_output(upload_list_comd)
			print(t)
			#time.sleep(20)
	print('>>>feed uploaded ready for DW load')
	

ziped_report = fetch_data()
unzip_report(ziped_report)
parse_files()

test_tracks(track_rows)
test_albums(album_rows)
#upload_data()

