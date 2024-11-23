import streamlit as st
import pandas as pd
import requests
from curl_cffi import requests as curl_requests
import os
import re
import PyPDF2
import logging
import concurrent.futures
import datetime
from tqdm import tqdm
import io
import pikepdf
import shutil

# 设置日志
log_path = f'{os.sep}'.join(os.path.abspath(__file__).split(os.sep)[:-1]) + os.sep + '占知文件批量下载工作日志.log'
logging.basicConfig(filename=log_path, level=logging.INFO, encoding='utf-8')

# 固定下载路径
DOWNLOAD_PATH = os.path.dirname(os.path.abspath(__file__))+os.sep+"Downloads"
if not os.path.exists(DOWNLOAD_PATH):
    os.makedirs(DOWNLOAD_PATH)
else:
    shutil.remtree(DOWNLOAD_PATH)
    os.makedirs(DOWNLOAD_PATH)

def download_files(df, title_field, url_field):
    item_list = [[name, link, DOWNLOAD_PATH] for name, link in zip(df[title_field], df[url_field])]
    try:
        is_down_list = df['是否下载成功'].to_list()
    except KeyError:
        is_down_list = [False for _ in range(len(df))]
    
    for index, choice in enumerate(is_down_list):
        if choice != 'Y':
            if download_url(item_list[index]):
                is_down_list[index] = 'Y'
            else:
                is_down_list[index] = 'N'
    
    df['是否下载成功'] = is_down_list
    return df

def download_files2(df, title_field, url_field):
    item_list = [[name, link, DOWNLOAD_PATH] for name, link in zip(df[title_field], df[url_field])]
    try:
        is_down_list = df['是否下载成功'].to_list()
    except KeyError:
        is_down_list = ['null' for _ in range(len(df))]
    
    for index, choice in enumerate(is_down_list):
        if choice != 'Y':
            if download_url_with_curl(item_list[index]):
                is_down_list[index] = 'Y'
            else:
                is_down_list[index] = 'N'
    
    df['是否下载成功'] = is_down_list
    return df

def rename_files(prefix, use_metadata_page, use_metadata_year):
    item_list1 = [[DOWNLOAD_PATH, file, prefix, use_metadata_page, use_metadata_year] for file in os.listdir(DOWNLOAD_PATH)]
    with concurrent.futures.ThreadPoolExecutor(max_workers=3) as executor:
        executor.map(file_rename, item_list1)

def decrypt_files():
    pdf_path_list = [DOWNLOAD_PATH + os.sep + pdf_path for pdf_path in os.listdir(DOWNLOAD_PATH) if '.PDF' in pdf_path]
    with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
        executor.map(unlock_file, pdf_path_list)

def unlock_file(file):
    try:
        pdf = pikepdf.open(file, allow_overwriting_input=True)
        pdf.save(file)
        st.write(f'{file.split(os.sep)[-1]} 破解成功！')
    except Exception as decrypterror:
        logging.info(f'{datetime.datetime.now()} {file} decrypterror: {decrypterror}!')

def download_url(item: list) -> bool:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        }
        name = str(item[0])
        name = re.sub(r'[\/\\\:\*\?\"\<\>\|]', '_', name)
        if not os.path.exists(f'{item[2]}{os.sep}{name}.PDF'):
            try:
                resp = requests.get(str(item[1]), headers=headers, timeout=60000, stream=True, verify=False)
                bytes_io = io.BytesIO(resp.content)
                if resp.status_code == 200:
                    try:
                        file_size = int(resp.headers.get('Content-Length'))
                    except:
                        file_size = len(resp.content)
                    pbar = tqdm(total=file_size)
                    with open(f'{item[2]}{os.sep}{name}.PDF', mode='wb') as f:
                        for chunk in bytes_io:
                            if chunk:
                                f.write(chunk)
                                pbar.set_description(f'{name} downloading...')
                                pbar.update(len(chunk))
                        return 'Y'
                else:
                    st.write(f'{name} failed because of response status {resp.status_code}')
                    return 'N'
            except Exception as e:
                st.write(f'{datetime.datetime.now()} 通道1 {item[1]}发生错误!{e}')
                logging.info(f'{datetime.datetime.now()} 通道1 {item[1]}发生错误!{e}')
                return 'N'
    except Exception as e2:
        st.write(f'{datetime.datetime.now()} 通道1 {item[1]}发生错误!{e2}')
        logging.info(f'{datetime.datetime.now()} 通道1 {item[1]}发生错误!{e2}')
        return 'N'

def download_url_with_curl(item: list) -> bool:
    try:
        headers = {
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/119.0.0.0 Safari/537.36',
        }
        name = str(item[0])
        name = re.sub(r'[\/\\\:\*\?\"\<\>\|]', '_', name)
        st.write(f'{item[2]}{os.sep}{name}.PDF')
        if not os.path.exists(f'{item[2]}{os.sep}{name}.PDF'):
            try:
                resp = curl_requests.get(str(item[1]), headers=headers, timeout=60000, impersonate='chrome101', verify=False)
                if resp.status_code == 200:
                    bytes_io = io.BytesIO(resp.content)
                    try:
                        file_size = int(resp.headers.get('Content-Length'))
                    except:
                        file_size = len(resp.content)
                    pbar = tqdm(total=file_size)
                    with open(f'{item[2]}{os.sep}{name}.PDF', mode='wb') as f:
                        for chunk in bytes_io:
                            if chunk:
                                pbar.set_description(f'{name} downloading...')
                                pbar.update(f.write(chunk))
                    return 'Y'
                else:
                    st.write(f'{datetime.datetime.now()} 通道2 {name} failed because of response status {resp.status_code}')
                    return 'N'
            except Exception as e:
                logging.info(f'{datetime.datetime.now()} 通道2 {item[1]} 发生错误!{e}')
                return 'N'
    except Exception as e2:
        logging.info(f'{datetime.datetime.now()} 通道2 {item[1]}发生错误!{e2}')
        return False

def zip_folder(folder_path, zip_name):
    shutil.make_archive(zip_name, 'zip', folder_path)
    return f"{zip_name}.zip"

def file_rename(item_list1: list):
    if '.PDF' in item_list1[1]:
        item_list1[0] = os.path.normpath(item_list1[0])
        try:
            old_path = item_list1[0] + os.sep + item_list1[1]
            pdf_file = open(old_path, 'rb')
            if item_list1[3]:
                page_number = len(PyPDF2.PdfReader(pdf_file).pages)
            else:
                page_number = ''
            if item_list1[4]:
                year = str(PyPDF2.PdfReader(pdf_file).metadata.creation_date).split('-')[0]
            else:
                year = ''
            pdf_file.close()
            if item_list1[2] != '' and f'{item_list1[2]}：' in item_list1[1]:
                return
            elif f'{page_number}页' in item_list1[1] or f'({year})' in item_list1[1]:
                return
            if item_list1[2] != '' and page_number != '' and year != '':
                new_path = item_list1[0] + os.sep + item_list1[2] + re.sub('.PDF', f'({year}) {page_number}页.PDF', item_list1[1])
            elif item_list1[2] != '' and page_number != '' and year == '':
                new_path = item_list1[0] + os.sep + item_list1[2] + re.sub('.PDF', f' {page_number}页.PDF', item_list1[1])
            elif item_list1[2] != '' and page_number == '' and year == '':
                new_path = item_list1[0] + os.sep + item_list1[2] + item_list1[1]
            os.rename(old_path, new_path)
            logging.info(f'{datetime.datetime.now()} {old_path}-->{new_path} down!')
        except Exception as e:
            logging.info(f'{datetime.datetime.now()} Error occurred while renaming {old_path}: {e}')

# Streamlit 界面
st.title("占知批量下载工具")

# 上传 Excel 文件
uploaded_file = st.file_uploader("上传 Excel 文件", type=["xlsx", "xls"])
if uploaded_file is not None:
    df = pd.read_excel(uploaded_file).fillna('')
    st.write("上传的 Excel 文件内容：")
    st.write(df)

    # 输入字段
    title_field = st.text_input("标题字段")
    url_field = st.text_input("URL链接字段")

    # 下载按钮
    if st.button("通道1下载"):
        df = download_files(df, title_field, url_field)
        st.write("下载完成")
        st.write(df)

    if st.button("通道2下载"):
        df = download_files2(df, title_field, url_field)
        st.write("下载完成")
        st.write(df)

    # 重命名模块
    st.header("重命名模块")
    prefix = st.text_input("文件名前缀")
    use_metadata_page = st.checkbox("读取PDF页数元数据")
    use_metadata_year = st.checkbox("读取PDF年份元数据")

    if st.button("重命名"):
        rename_files(prefix, use_metadata_page, use_metadata_year)
        st.write("重命名完成")

    # PDF破解模块
    st.header("PDF破解模块")
    if st.button("PDF批量破解"):
        decrypt_files()
        st.write("PDF破解完成")
    
    st.header("下载PDF文件夹")
    if st.button("生成并下载PDF文件夹压缩包"):
        zip_path = zip_folder(DOWNLOAD_PATH, "PDF_Files")
        with open(zip_path, "rb") as zip_file:
            st.download_button(
                label="下载PDF文件夹压缩包",
                data=zip_file,
                file_name="PDF_Files.zip",
                mime="application/zip"
            )
