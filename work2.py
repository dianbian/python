#! /usr/bin/env python
# -*- coding: utf8 -*-

import httplib
import urllib2
import json
import MySQLdb
import datetime
import time
import subprocess
import os
import requests
import logging
import csv
import json
import re
import multiprocessing


from promisesetao.stub import *
from promisesetao.DeliveryRulePo_xxo import * 
from bbcplatform import web_stub_cntl

import sys
reload(sys)
sys.setdefaultencoding("utf-8")

PEERIP = "172.172.178.83" 

#IDC
strUrl = 'https://test.kwms.haiziwang.com/api/delivery/delivery-time/whNo?'

strFilePath = './log'
INVENTORY_TBNUM = 1024

global g_fileName
global logger
#  日志模块
def logInit(log_file):
	logger = logging.getLogger('c')
	logger.setLevel(logging.DEBUG)
	ch = logging.FileHandler(log_file,'a')
	ch.setLevel(logging.DEBUG)
	logger.addHandler(ch)
	return logger

##调接口
def DoInvoke(req,resp):
	if req.machineKey in [None,""]:
		req.machineKey = "local"
	if req.source in [None,""]:
		req.source = "python_tool"
	if req.sceneId in [None,0]:
		req.sceneId = 100
	if req.option in [None,0]:
		req.option = 50	
	try:
		stub = web_stub_cntl.WebStubCntl()
		stub.setPeerIPPort(PEERIP, 53101)
		stub.invoke(req,resp)
		if resp.result == 0:
			logger.debug('call success')
			return True
		else:
			logger.error('call failed code[%d]'%(resp.result))
			return False
	except Exception as e:
		logger.error(e)
		return False	
# 解析json
def DoParseJson(jsonArray):
	logger.debug('DoParseJson')
	jsonDict = json.loads(jsonArray, encoding=None)
	print('axxxxxxyyyyyyyyyyyyy')
	if jsonDict.has_key('resultCode') and jsonDict['resultCode'] == '1' and jsonDict.has_key('data') and len(jsonDict['data']) > 0:
		print('mmmmmmmmmmmmmmmmmmm')
		data = jsonDict['data']
		dataLen = len(jsonDict['data'])
		print(dataLen)
		index = dataLen / 100 + 1  #least 1
		for i in range(index):
			j = i * 100
			vecRule = stl_vector('promisesetao.DeliveryRulePo_xxo.DeliveryRulePo') 
			for j in range(j, j+100):
				if j >= dataLen:
					break
				deliveryRulePo = DeliveryRulePo() #define var
				deliveryRulePo.ddwStockSysno = int(data[j]['whNo'])
 				deliveryRulePo.cStockSysno_u = 1
				deliveryRulePo.strDeliveryName = str(data[j]['carrierName'])
				deliveryRulePo.cDeliveryName_u = 1
				deliveryRulePo.strAddress = str(data[j]['rcProvinceNo']) + '_' + str(data[j]['rcCityNo']) + '_' + str(data[j]['rcDistrictNo']) 
				deliveryRulePo.cAddress_u = 1
				deliveryRulePo.dwState = int(data[j]['status'])
				deliveryRulePo.cState_u = 1
				if int(data[j]['status']) == 1:
					deliveryRulePo.dwDeliveryTime = int(data[j]['deliveryTime'])
					deliveryRulePo.strReserveStr = str(data[j]['statusDesc'])
				else:
					deliveryRulePo.dwDeliveryTime = 99999
					deliveryRulePo.strReserveStr = 'p202/5/9'
				deliveryRulePo.cDeliveryTime_u = 1
				deliveryRulePo.strLastModifier = 'python_kwms'
				deliveryRulePo.cLastModifier_u = 1
				deliveryRulePo.cReserveStr_u = 1
				if (len(deliveryRulePo.strAddress) < 20):
					continue
				vecRule.append(deliveryRulePo)
			ret = DoUpdateDeliveryRuleReq(vecRule) # 100
			print('in = ' + str(len(vecRule)) + ' out = ' + str(ret))

# 读取线上子商品库存信息
def DoUpdateDeliveryRuleReq(vecRule):
	logger.debug('UpdateDeliveryRuleReq ')
	req = UpdateDeliveryRuleReq()  #请求接口和参数
	resp = UpdateDeliveryRuleResp() #返回接口
	req.deliveryRuleIn = vecRule
	req.machineKey = "simida"
	req.source = "listenKwms"
	req.sceneId = 333
	req.option = 777
	req.inReserve = ""

	res = DoInvoke(req, resp)
	if res == False:
		logger.debug('req failed ' + str(vecRule))
		return 0
	else:
		
		logger.debug('req success ' + str(len(vecRule)))
		return len(resp.deliveryRuleOut)


def nowDatetimeStr():
	ticks = time.time()
	timeArray = time.localtime(int(ticks))
	return time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

#推http
def pushSearch(url, sysno):
	try:
		#params = {"whNo":sysno}
		sUrl= url + 'whNo=' + str(sysno) 
		r = requests.post(url=sUrl, verify=False)
		if r.status_code == requests.codes.ok:
			return r.text
		else:
			print('xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx\n')

	except Exception as e:
		logger.debug(nowDatetimeStr() + ' error, exception ' + str(e))

starttime = time.time()
print starttime
g_fileName = "httpKwms"
strFileName = strFilePath +'/'+ g_fileName+'_'+ datetime.datetime.now().strftime('%Y-%m-%d') + '.log'
logger = logInit(strFileName) 

#主函数开始
try:
	logger.debug('******************http for kwms*************start time:' + nowDatetimeStr())	
	csv_read = [9103]
	index = 0
	for data in csv_read: #数据提取
		text = pushSearch(strUrl, data)
		DoParseJson(text)
		index = index + 1

	logger.debug('****************** http for kwms**********end time:' + nowDatetimeStr() + ' index=' + str(index))		
	print "count : " + str(index)
except MySQLdb.Error,e:
	print "Mysql Error %d: %s" % (e.args[0], e.args[1])
		
endtime = time.time()
print endtime
print endtime - starttime
