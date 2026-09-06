import os
import json
import asyncio
import httpx
import websockets
import pandas as pd
import feedparser
from io import StringIO
from datetime import datetime, timezone
from supabase import create_client, Client
import math
from dotenv import load_dotenv
from curl_cffi.requests import AsyncSession
from curl_cffi import requests
import websockets
from bs4 import BeautifulSoup
import xml.etree.ElementTree as ET
import math
import time
import re
import random
import hashlib
from collections import defaultdict
import boto3
load_dotenv()

GLOBAL_MAP = {
    "AFG": [33.9, 67.7], "ALB": [41.1, 20.1], "DZA": [28.0, 1.6], "AND": [42.5, 1.5], "AGO": [-11.2, 17.8], 
    "ARG": [-38.4, -63.6], "ARM": [40.0, 45.0], "AUS": [-25.2, 133.7], "AUT": [47.5, 14.5], "AZE": [40.1, 47.5], 
    "BHS": [25.0, -77.3], "BHR": [26.0, 50.5], "BGD": [23.6, 90.3], "BRB": [13.1, -59.5], "BLR": [53.7, 27.9], 
    "BEL": [50.5, 4.4], "BLZ": [17.1, -88.4], "BEN": [9.3, 2.3], "BTN": [27.5, 90.4], "BOL": [-16.2, -63.5], 
    "BIH": [43.9, 17.6], "BWA": [-22.3, 24.6], "BRA": [-14.2, -51.9], "BRN": [4.5, 114.7], "BGR": [42.7, 25.4], 
    "BFA": [12.2, -1.5], "BDI": [-3.3, 29.9], "KHM": [12.5, 104.9], "CMR": [7.3, 12.3], "CAN": [56.1, -106.3], 
    "CPV": [16.0, -24.0], "CAF": [6.6, 20.9], "TCD": [15.4, 18.7], "CHL": [-35.6, -71.5], "CHN": [35.8, 104.1], 
    "COL": [4.5, -74.2], "COM": [-11.6, 43.3], "COG": [-0.2, 15.8], "COD": [-4.0, 21.7], "CRI": [9.7, -83.7], 
    "HRV": [45.1, 15.2], "CUB": [21.5, -77.7], "CYP": [35.1, 33.4], "CZE": [49.8, 15.4], "DNK": [56.2, 9.5], 
    "DJI": [11.8, 42.5], "DMA": [15.4, -61.3], "DOM": [18.7, -70.1], "ECU": [-1.8, -78.1], "EGY": [26.8, 30.8], 
    "SLV": [13.7, -88.8], "GNQ": [1.6, 10.2], "ERI": [15.1, 39.7], "EST": [58.5, 25.0], "ETH": [9.1, 40.4], 
    "FJI": [-17.7, 178.0], "FIN": [61.9, 25.7], "FRA": [46.2, 2.2], "GAB": [-0.8, 11.6], "GMB": [13.4, -15.3], 
    "GEO": [42.3, 43.3], "DEU": [51.1, 10.4], "GHA": [7.9, -1.0], "GRC": [39.0, 21.8], "GRD": [12.1, -61.6], 
    "GTM": [15.7, -90.2], "GIN": [9.9, -9.6], "GNB": [11.8, -15.1], "GUY": [4.8, -58.9], "HTI": [18.9, -72.6], 
    "HND": [15.1, -86.2], "HUN": [47.1, 19.5], "ISL": [64.9, -18.1], "IND": [20.5, 78.9], "IDN": [-0.7, 113.9], 
    "IRN": [32.4, 53.6], "IRQ": [33.2, 43.6], "IRL": [53.4, -8.2], "ISR": [31.0, 34.8], "ITA": [41.8, 12.5], 
    "JAM": [18.1, -77.2], "JPN": [36.2, 138.2], "JOR": [30.5, 36.2], "KAZ": [48.0, 66.9], "KEN": [-0.0, 37.9], 
    "KIR": [-3.3, -168.7], "KOR": [35.9, 127.7], "KWT": [29.3, 47.4], "KGZ": [41.2, 74.7], "LAO": [19.8, 102.4], 
    "LVA": [56.8, 24.6], "LBN": [33.8, 35.8], "LSO": [-29.6, 28.2], "LBR": [6.4, -9.4], "LBY": [26.3, 17.2], 
    "LIE": [47.1, 9.5], "LTU": [55.1, 23.8], "LUX": [49.8, 6.1], "MKD": [41.6, 21.7], "MDG": [-18.7, 46.8], 
    "MWI": [-13.2, 34.3], "MYS": [4.2, 101.9], "MDV": [3.2, 73.2], "MLI": [17.5, -3.9], "MLT": [35.9, 14.3], 
    "MHL": [7.1, 171.1], "MRT": [21.0, -10.9], "MUS": [-20.3, 57.5], "MEX": [23.6, -102.5], "FSM": [7.4, 151.8], 
    "MDA": [47.4, 28.3], "MCO": [43.7, 7.4], "MNG": [46.8, 103.8], "MNE": [42.7, 19.3], "MAR": [31.7, -7.0], 
    "MOZ": [-18.6, 35.5], "MMR": [21.9, 95.9], "NAM": [-22.9, 18.4], "NRU": [-0.5, 166.9], "NPL": [28.3, 84.1], 
    "NLD": [52.1, 5.2], "NZL": [-40.9, 174.8], "NIC": [12.8, -85.2], "NER": [17.6, 8.0], "NGA": [9.0, 8.6], 
    "NOR": [60.4, 8.4], "OMN": [21.5, 55.9], "PAK": [30.3, 69.3], "PLW": [7.5, 134.5], "PAN": [8.5, -80.7], 
    "PNG": [-6.3, 143.9], "PRY": [-23.4, -58.4], "PER": [-9.1, -75.0], "PHL": [12.8, 121.7], "POL": [51.9, 19.1], 
    "PRT": [39.3, -8.2], "QAT": [25.3, 51.1], "ROU": [45.9, 24.9], "RUS": [61.5, 105.3], "RWA": [-1.9, 29.8], 
    "KNA": [17.3, -62.7], "LCA": [13.9, -60.9], "VCT": [12.9, -61.2], "WSM": [-13.7, -172.1], "SMR": [43.9, 12.4], 
    "STP": [0.1, 6.6], "SAU": [23.8, 45.0], "SEN": [14.4, -14.4], "SRB": [44.0, 21.0], "SYC": [-4.6, 55.4], 
    "SLE": [8.4, -11.7], "SGP": [1.3, 103.8], "SVK": [48.6, 19.6], "SVN": [46.1, 14.9], "SLB": [-9.6, 160.1], 
    "SOM": [5.1, 46.1], "ZAF": [-30.5, 22.9], "SSD": [6.8, 31.3], "ESP": [40.4, -3.7], "LKA": [7.8, 80.7], 
    "SDN": [12.8, 30.2], "SUR": [3.9, -56.0], "SWZ": [-26.5, 31.4], "SWE": [60.1, 18.6], "CHE": [46.8, 8.2], 
    "SYR": [34.8, 39.0], "TWN": [23.6, 120.9], "TJK": [38.8, 71.2], "TZA": [-6.3, 34.8], "THA": [15.8, 100.9], 
    "TLS": [-8.8, 125.7], "TGO": [8.6, 0.8], "TON": [-21.1, -175.1], "TTO": [10.6, -61.2], "TUN": [33.8, 9.5], 
    "TUR": [38.9, 35.2], "TKM": [38.9, 59.5], "TUV": [-7.1, 177.6], "UGA": [1.3, 32.2], "UKR": [48.3, 31.1], 
    "ARE": [23.4, 53.8], "GBR": [55.3, -3.4], "USA": [37.0, -95.7], "URY": [-32.5, -55.7], "UZB": [41.3, 64.5], 
    "VUT": [-15.3, 166.9], "VEN": [6.4, -66.5], "VNM": [14.0, 108.2], "YEM": [15.5, 48.5], "ZMB": [-13.1, 27.8], "ZWE": [-19.0, 29.1]
}

class GeoIntelligenceLayerUpdater:
    def __init__(self):
        # API CREDENTIALS
        self.ais_key = "999fab98e4a71a72295d86cd91286235cef266f3"
        self.firms_key = os.getenv("NASA_FIRMS_KEY")
        self.supabase: Client = create_client(os.getenv("SUPABASE_URL"), os.getenv("SUPABASE_SERVICE_ROLE_KEY"))
        self.session = AsyncSession(impersonate="chrome120")
        self.headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
        }
        self.ISO_MAP = {
            "AF": [33.939, 67.710], "AL": [41.153, 20.168], "DZ": [28.034, 1.660], "AS": [-14.271, -170.132],
            "AD": [42.546, 1.602], "AO": [-11.203, 17.874], "AI": [18.221, -63.069], "AQ": [-75.251, -0.071],
            "AG": [17.061, -61.796], "AR": [-38.416, -63.617], "AM": [40.069, 45.038], "AW": [12.521, -69.968],
            "AU": [-25.274, 133.775], "AT": [47.516, 14.550], "AZ": [40.143, 47.577], "BS": [25.034, -77.396],
            "BH": [25.930, 50.638], "BD": [23.685, 90.356], "BB": [13.194, -59.543], "BY": [53.710, 27.953],
            "BE": [50.504, 4.470], "BZ": [17.189, -88.498], "BJ": [9.308, 2.316], "BM": [32.308, -64.751],
            "BT": [27.514, 90.434], "BO": [-16.290, -63.589], "BA": [43.916, 17.679], "BW": [-22.328, 24.685],
            "BR": [-14.235, -51.925], "BN": [4.535, 114.728], "BG": [42.734, 25.486], "BF": [12.238, -1.562],
            "BI": [-3.373, 29.919], "KH": [12.566, 104.991], "CM": [7.370, 12.355], "CA": [56.130, -106.347],
            "CV": [16.002, -24.013], "KY": [19.513, -80.567], "CF": [6.611, 20.939], "TD": [15.454, 18.732],
            "CL": [-35.675, -71.543], "CN": [35.862, 104.195], "CO": [4.571, -74.297], "KM": [-11.646, 43.333],
            "CG": [-0.228, 15.828], "CD": [-4.038, 21.759], "CR": [9.749, -83.753], "CI": [7.540, -5.547],
            "HR": [45.100, 15.200], "CU": [21.522, -77.781], "CY": [35.126, 33.430], "CZ": [49.817, 15.473],
            "DK": [56.264, 9.502], "DJ": [11.825, 42.590], "DO": [18.736, -70.163], "EC": [-1.831, -78.183],
            "EG": [26.821, 30.803], "SV": [13.794, -88.897], "GQ": [1.651, 10.268], "ER": [15.179, 39.782],
            "EE": [58.595, 25.014], "ET": [9.145, 40.489], "FJ": [-16.578, 179.414], "FI": [61.924, 25.748],
            "FR": [46.228, 2.214], "GA": [-0.804, 11.609], "GM": [13.443, -15.310], "GE": [42.315, 43.357],
            "DE": [51.166, 10.452], "GH": [7.947, -1.023], "GR": [39.074, 21.824], "GL": [71.707, -42.604],
            "GD": [12.117, -61.679], "GU": [13.444, 144.794], "GT": [15.783, -90.231], "GN": [9.946, -9.697],
            "GW": [11.804, -15.180], "GY": [4.860, -58.930], "HT": [18.971, -72.285], "HN": [15.200, -86.242],
            "HK": [22.396, 114.109], "HU": [47.162, 19.503], "IS": [64.963, -19.021], "IN": [20.594, 78.963],
            "ID": [-0.789, 113.921], "IR": [32.428, 53.688], "IQ": [33.223, 43.679], "IE": [53.413, -8.244],
            "IL": [31.046, 34.852], "IT": [41.872, 12.567], "JM": [18.109, -77.298], "JP": [36.205, 138.253],
            "JO": [30.585, 36.238], "KZ": [48.020, 66.924], "KE": [-0.024, 37.906], "KP": [40.340, 127.510],
            "KR": [35.908, 127.767], "KW": [29.312, 47.482], "KG": [41.204, 74.766], "LA": [19.856, 102.495],
            "LV": [56.880, 24.603], "LB": [33.855, 35.862], "LS": [-29.610, 28.234], "LR": [6.428, -9.429],
            "LY": [26.335, 17.228], "LT": [55.169, 23.881], "LU": [49.815, 6.130], "MO": [22.199, 113.544],
            "MK": [41.609, 21.745], "MG": [-18.767, 46.869], "MW": [-13.254, 34.302], "MY": [4.211, 101.976],
            "MV": [3.203, 73.221], "ML": [17.570, -3.996], "MT": [35.938, 14.375], "MH": [7.131, 171.184],
            "MR": [21.008, -10.941], "MU": [-20.348, 57.552], "MX": [23.635, -102.553], "MD": [47.412, 28.370],
            "MN": [46.862, 103.847], "ME": [42.709, 19.374], "MA": [31.792, -7.093], "MZ": [-18.666, 35.530],
            "MM": [21.916, 95.956], "NA": [-22.958, 18.490], "NP": [28.395, 84.124], "NL": [52.133, 5.291],
            "NZ": [-40.901, 174.886], "NI": [12.865, -85.207], "NE": [17.608, 8.082], "NG": [9.082, 8.675],
            "NO": [60.472, 8.469], "OM": [21.513, 55.923], "PK": [30.375, 69.345], "PS": [31.952, 35.233],
            "PA": [8.538, -80.782], "PG": [-6.315, 143.956], "PY": [-23.443, -58.444], "PE": [-9.190, -75.015],
            "PH": [12.880, 121.774], "PL": [51.919, 19.145], "PT": [39.399, -8.224], "QA": [25.355, 51.184],
            "RO": [45.943, 24.967], "RU": [61.524, 105.319], "RW": [-1.940, 29.874], "SA": [23.885, 45.079],
            "SN": [14.497, -14.452], "RS": [44.017, 21.006], "SG": [1.352, 103.820], "SK": [48.669, 19.699],
            "SI": [46.151, 14.995], "SO": [5.152, 46.199], "ZA": [-30.559, 22.938], "ES": [40.464, -3.749],
            "LK": [7.873, 80.772], "SD": [12.863, 30.218], "SE": [60.128, 18.644], "CH": [46.818, 8.228],
            "SY": [34.802, 38.997], "TW": [23.698, 120.961], "TJ": [38.861, 71.276], "TZ": [-6.369, 34.889],
            "TH": [15.870, 100.993], "TL": [-8.874, 125.728], "TG": [8.619, 0.825], "TN": [33.887, 9.538],
            "TR": [38.964, 35.243], "TM": [38.970, 59.557], "UG": [1.373, 32.290], "UA": [48.379, 31.166],
            "AE": [23.424, 53.848], "GB": [55.378, -3.436], "US": [37.090, -95.713], "UY": [-32.523, -55.766],
            "UZ": [41.377, 64.585], "VE": [6.424, -66.590], "VN": [14.058, 108.277], "YE": [15.553, 48.516],
            "ZM": [-13.134, 27.849], "ZW": [-19.015, 29.155]
        }
        # THE STYLING MATRIX (Replaces GLBs with High-Performance UI Configs)
        self.themes = {
            "mil_air": {"emoji": "✈️", "img": "https://images.defense.gov/f35_thumb.jpg", "color": "#00F0FF"},
            "comm_air": {"emoji": "🛫", "img": "https://example.com/b777.jpg", "color": "#FFFFFF"},
            "warship": {"emoji": "🚢", "img": "https://navy.mil/cvn_class.jpg", "color": "#39FF14"},
            "dark_fleet": {"emoji": "🏴‍☠️", "img": "https://example.com/tanker.jpg", "color": "#800080"},
            "spy_sat": {"emoji": "🛰️", "img": "https://example.com/sat.jpg", "color": "#FF00FF"},
            "rocket": {"emoji": "🚀", "img": "https://example.com/rocket.jpg", "color": "#FF4500"},
            "drone": {"emoji": "🚁", "img": "https://example.com/reaper.jpg", "color": "#FFA500"},
            "rail": {"emoji": "🚂", "img": "https://example.com/freight.jpg", "color": "#A9A9A9"},
            "kinetic": {"emoji": "🔥", "img": "https://example.com/impact.jpg", "color": "#FF0000"},
            "ew_jam": {"emoji": "📡", "img": "https://example.com/radar.jpg", "color": "#FFFF00"},
            "cyber": {"emoji": "⚡", "img": "https://example.com/server.jpg", "color": "#00FF00"},
            "seismic": {"emoji": "💥", "img": "https://example.com/quake.jpg", "color": "#8B0000"},
            "radiation": {"emoji": "☢️", "img": "https://example.com/nuke_plant.jpg", "color": "#7FFF00"},
            "notam": {"emoji": "🛑", "img": "https://example.com/airspace.jpg", "color": "#FF1493"},
            "pipeline": {"emoji": "⛽", "img": "https://example.com/pipe.jpg", "color": "#FFD700"},
            "cable": {"emoji": "🔌", "img": "https://example.com/sub_cable.jpg", "color": "#00BFFF"},
            "airbase": {"emoji": "🛬", "img": "https://example.com/base.jpg", "color": "#808080"},
            "nuke": {"emoji": "☢️", "img": "https://example.com/reactor.jpg", "color": "#39FF14"},
            "mine": {"emoji": "⛏️", "img": "https://example.com/mine.jpg", "color": "#B87333"},
            "chokepoint": {"emoji": "⚓", "img": "https://example.com/strait.jpg", "color": "#000080"},
            "datacenter": {"emoji": "🗄️", "img": "https://example.com/server.jpg", "color": "#00FFFF"},
            "sanction": {"emoji": "📜", "img": "https://example.com/embargo.jpg", "color": "#FFD700"},
            "finance": {"emoji": "💱", "img": "https://example.com/bank.jpg", "color": "#00FF00"},
            "border": {"emoji": "🚧", "img": "https://example.com/fence.jpg", "color": "#FF8C00"},
            "arms": {"emoji": "📦", "img": "https://example.com/cargo.jpg", "color": "#8B4513"},
            "hangar": {"emoji": "🏭", "img": "https://example.com/hangar.jpg", "color": "#696969"},
            "undersea_quake": {"emoji": "🌊", "img": "https://example.com/tsunami.jpg", "color": "#4B0082"},
            "csg_radius": {"emoji": "🎯", "img": "https://example.com/radar_ring.jpg", "color": "#00BFFF"},
            "sonar": {"emoji": "🎛️", "img": "https://example.com/buoy.jpg", "color": "#1E90FF"},
            "sat_cone": {"emoji": "👁️", "img": "https://example.com/fov.jpg", "color": "#FF69B4"},
            "sar_scan": {"emoji": "📸", "img": "https://example.com/sar.jpg", "color": "#FFFFFF"},
            "alert": {"emoji": "🚨", "img": "https://example.com/siren.jpg", "color": "#FF0000"},
            "density": {"emoji": "📱", "img": "https://example.com/crowd.jpg", "color": "#FFFF00"},
            "swarm": {"emoji": "🐝", "img": "https://example.com/swarm.jpg", "color": "#FF4500"},
            "stealth": {"emoji": "👻", "img": "https://example.com/stealth.jpg", "color": "#2F4F4F"},
            "vip": {"emoji": "🕴️", "img": "https://example.com/convoy.jpg", "color": "#DAA520"}
        }
        self.ZONES = {
            # --- NORTH AMERICA ---
            "NYC": {"bl": "40.50,-74.25", "tr": "40.90,-73.70"},
            "LA": {"bl": "33.70,-118.60", "tr": "34.30,-118.10"},
            "CHICAGO": {"bl": "41.60,-87.90", "tr": "42.00,-87.50"},
            "DC": {"bl": "38.80,-77.15", "tr": "39.00,-76.90"},
            "TORONTO": {"bl": "43.60,-79.60", "tr": "43.90,-79.10"},
            "MEXICO_CITY": {"bl": "19.10,-99.40", "tr": "19.60,-98.90"},
            "MIAMI": {"bl": "25.70,-80.40", "tr": "25.90,-80.10"},
            "HOUSTON": {"bl": "29.50,-95.60", "tr": "30.00,-95.10"},
            "SEATTLE": {"bl": "47.40,-122.50", "tr": "47.80,-122.20"},
            "SAN_FRAN": {"bl": "37.60,-122.60", "tr": "37.90,-122.30"},

            # --- EUROPE ---
            "LONDON": {"bl": "51.30,-0.30", "tr": "51.70,0.20"},
            "PARIS": {"bl": "48.75,2.15", "tr": "48.95,2.55"},
            "BERLIN": {"bl": "52.35,13.10", "tr": "52.65,13.70"},
            "MADRID": {"bl": "40.30,-3.90", "tr": "40.60,-3.50"},
            "ROME": {"bl": "41.70,12.30", "tr": "42.10,12.80"},
            "AMSTERDAM": {"bl": "52.25,4.75", "tr": "52.45,5.05"},
            "BRUSSELS": {"bl": "50.75,4.25", "tr": "50.95,4.50"},
            "WARSAW": {"bl": "52.10,20.80", "tr": "52.35,21.20"},
            "VIENNA": {"bl": "48.10,16.20", "tr": "48.35,16.55"},
            "KYIV": {"bl": "50.35,30.30", "tr": "50.60,30.70"},
            "MOSCOW": {"bl": "55.50,37.30", "tr": "55.95,37.95"},

            # --- ASIA & OCEANIA ---
            "TOKYO": {"bl": "35.50,139.50", "tr": "35.85,139.95"},
            "SEOUL": {"bl": "37.40,126.80", "tr": "37.70,127.20"},
            "SHANGHAI": {"bl": "31.00,121.10", "tr": "31.50,121.90"},
            "BEIJING": {"bl": "39.75,116.10", "tr": "40.15,116.70"},
            "HONG_KONG": {"bl": "22.20,114.00", "tr": "22.50,114.30"},
            "SINGAPORE": {"bl": "1.20,103.60", "tr": "1.50,104.10"},
            "BANGKOK": {"bl": "13.50,100.30", "tr": "14.00,100.80"},
            "MUMBAI": {"bl": "18.90,72.70", "tr": "19.30,73.00"},
            "DELHI": {"bl": "28.40,76.90", "tr": "28.80,77.40"},
            "JAKARTA": {"bl": "-6.40,106.60", "tr": "-6.00,107.00"},
            "SYDNEY": {"bl": "-34.10,150.80", "tr": "-33.60,151.40"},
            "MELBOURNE": {"bl": "-38.10,144.70", "tr": "-37.60,145.20"},
            "TAIPEI": {"bl": "24.95,121.40", "tr": "25.20,121.70"},

            # --- MIDDLE EAST & AFRICA ---
            "DUBAI": {"bl": "24.90,54.90", "tr": "25.40,55.60"},
            "RIYADH": {"bl": "24.50,46.50", "tr": "25.00,47.00"},
            "TEL_AVIV": {"bl": "32.00,34.70", "tr": "32.20,34.90"},
            "CAIRO": {"bl": "29.90,31.10", "tr": "30.20,31.50"},
            "ISTANBUL": {"bl": "40.80,28.50", "tr": "41.30,29.60"},
            "TEHRAN": {"bl": "35.60,51.20", "tr": "35.85,51.60"},
            "JOHANNESBURG": {"bl": "-26.40,27.80", "tr": "-26.00,28.30"},
            "LAGOS": {"bl": "6.35,3.20", "tr": "6.70,3.65"},
            "NAIROBI": {"bl": "-1.40,36.60", "tr": "-1.10,37.10"},
            "CASABLANCA": {"bl": "33.45,-7.75", "tr": "33.70,-7.45"},

            # --- SOUTH AMERICA ---
            "SAO_PAULO": {"bl": "-24.00,-46.85", "tr": "-23.35,-46.30"},
            "BUENOS_AIRES": {"bl": "-34.75,-58.55", "tr": "-34.50,-58.30"},
            "BOGOTA": {"bl": "4.50,-74.25", "tr": "4.85,-74.00"},
            "LIMA": {"bl": "-12.20,-77.20", "tr": "-11.90,-76.90"},
            "SANTIAGO": {"bl": "-33.60,-70.85", "tr": "-33.30,-70.45"}
        }

    def _hex_to_rgba(self, hex_color):
        """Converts hex color string to RGBA list for CZML."""
        hex_color = hex_color.lstrip('#')
        return [int(hex_color[i:i+2], 16) for i in (0, 2, 4)] + [255]

    def _output_czml(self, layer_name, packets):
        """Builds a complete valid CZML document, prints it, and uploads to IDrive e2 bucket automatically."""
        doc = [{"id": "document", "name": f"VisionSphere - {layer_name.upper()}", "version": "1.0"}]
        doc.extend(packets)
        
        czml_string = json.dumps(doc, indent=2)
        
        # 1. Cleaner terminal log output (prevents giant payloads from flooding stdout)
        print(f"\n🎯 [VALID CZML OUTPUT] Layer: {layer_name.upper()} | Time: {datetime.now(timezone.utc).isoformat()}")
        print(f"   Generated {len(packets)} spatial assets. Routing to IDrive e2...")
        print(f"🏁 [END {layer_name.upper()}]\n")

        # 2. Automated upload to IDrive e2 bucket
        try:
            import boto3
            
            # Lazy initialize the client directly on the class to avoid changing your __init__ logic
            if not hasattr(self, 's3_client'):
                self.s3_client = boto3.client(
                    "s3",
                    endpoint_url=os.getenv("IDRIVE_ENDPOINT"),
                    aws_access_key_id=os.getenv("IDRIVE_ACCESS_KEY"),
                    aws_secret_access_key=os.getenv("IDRIVE_SECRET_KEY")
                )
                self.bucket_name = os.getenv("IDRIVE_BUCKET")
            
            # Generates uniquely named files separated by layer format
            filename = f"{layer_name.lower()}.czml"
            
            # Offload the synchronous upload call to a background thread so it doesn't freeze the scraper loop
            asyncio.create_task(asyncio.to_thread(
                self.s3_client.put_object,
                Bucket=self.bucket_name,
                Key=filename,
                Body=czml_string,
                ContentType="application/json"
            ))
            print(f"   [☁️ IDrive Cloud] Background upload task spawned: {filename}")
            
        except Exception as e:
            print(f"   [❌ IDrive Cloud Error] Upload configuration failed for {layer_name}: {e}")

    def _parse_line_route(self, route_string):
        """Parses GEM Route string into CZML-compatible degree list."""
        try:
            # Simple extractor: looks for float pairs in the text
            # Expects "lat, lon; lat, lon" or similar formats
            matches = re.findall(r"[-+]?\d*\.\d+|\d+", route_string)
            coords = []
            # Cesium expects [Lon, Lat, Alt, Lon, Lat, Alt...]
            for i in range(0, len(matches) - 1, 2):
                coords.extend([float(matches[i+1]), float(matches[i]), 0])
            return coords
        except:
            return None

    def _create_czml_packet(self, entity_id, name, lon, lat, alt, hex_color, type_desc, dossier_dict):
        """Transforms asset data into a Cesium-ready entity with labels and HTML popups."""
        
        # Fallback for name to ensure Cesium label doesn't break
        safe_name = name if name else "UNKNOWN_ASSET"
        display_label = f"{safe_name} [{type_desc}]" if type_desc else safe_name

        # Build HTML for the popup InfoBox
        html = f"<h3>{display_label}</h3><hr/>"
        for key, val in dossier_dict.get('telemetry', {}).items():
            html += f"<p><b>{str(key).upper()}:</b> {str(val)}</p>"
        
        summary = dossier_dict.get('strategic_summary')
        if summary:
            html += f"<hr/><p style='color:{hex_color};'><i>{summary}</i></p>"

        return {
            "id": str(entity_id),
            "name": display_label,
            "description": html,
            "position": {
                "cartographicDegrees": [lon, lat, alt]
            },
            "point": {
                "color": {"rgba": self._hex_to_rgba(hex_color)},
                "pixelSize": 12,
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 2
            },
            "label": {
                "text": display_label,
                "font": "12pt monospace",
                "style": "FILL_AND_OUTLINE",
                "horizontalOrigin": "LEFT",
                "pixelOffset": {"cartesian2": [15, 0]},
                "fillColor": {"rgba": [255, 255, 255, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 2
            }
        }

    # =================================================================
    # THE 5 CORE LAYERS (ROBUST IMPLEMENTATION)
    # =================================================================

    async def l01_mil_aviation(self):
        """L01: MILITARY AVIATION (ADSB.LOL)"""
        async with httpx.AsyncClient() as client:
            print("[L01] ✈️ Sweeping ADSB.lol for Military Transponders...")
            try:
                r = await client.get("https://api.adsb.lol/v2/mil", timeout=15)
                if r.status_code == 200:
                    aircraft = r.json().get('ac', [])
                    print(f"[L01] ✔️ Intercepted {len(aircraft)} military signatures.")
                        
                    packets = []
                    for ac in aircraft[:5]: # Debug slice
                        lon, lat = ac.get('lon'), ac.get('lat')
                            
                            # CRITICAL FALLBACK: Skip if no coords
                        if lon is None or lat is None:
                            print(f"[L01] ⚠️ Skipping {ac.get('hex')} - No GPS coordinates.")
                            continue
                            
                        # Extract specific model description
                        detailed_type = ac.get('desc') or ac.get('t') or "CLASSIFIED_AIRFRAME"
                        callsign = ac.get('callsign', 'NO_CALL').strip()
                            
                        dossier = {
                            "telemetry": {
                                "icao_hex": ac.get('hex'),
                                "model": detailed_type,
                                "altitude": f"{ac.get('alt_baro', 'Unknown')} ft",
                                "speed": f"{ac.get('gs', 0)} kts",
                                "registration": ac.get('r', 'Unknown')
                            },
                            "strategic_summary": "Active military flight detected."
                        }
                            
                        packet = self._create_czml_packet(
                            entity_id=ac.get('hex'),
                            name=callsign,
                            lon=lon, lat=lat, alt=ac.get('alt_baro', 0),
                            hex_color="#00F0FF", # Cyan
                            type_desc=detailed_type,
                            dossier_dict=dossier
                        )
                        packets.append(packet)
                        
                    if packets: self._output_czml("mil_air", packets)
                else:
                    print(f"[L01] ❌ API HTTP Error: {r.status_code}")
            except Exception as e:
                print(f"[L01] 💥 Critical Fail: {e}")
                

    async def l02_comm_aviation(self):
        """L02: COMMERCIAL AVIATION (OPENSKY)"""
        async with httpx.AsyncClient() as client:
            print("[L02] 🛫 Sweeping OpenSky for Commercial Corridors...")
            try:
                r = await client.get("https://opensky-network.org/api/states/all", timeout=20)
                if r.status_code == 200:
                    states = r.json().get('states', [])
                    print(f"[L02] ✔️ Extracted {len(states)} commercial states.")
                    
                    packets = []
                    for s in states[:5]: 
                        # OpenSky throws 'None' randomly. We must catch it.
                        if len(s) < 8 or s[5] is None or s[6] is None:
                            continue

                        icao = s[0]
                        callsign = s[1].strip() if s[1] else "UNKNOWN"
                        origin = s[2]
                        lon, lat, alt = s[5], s[6], s[7] or 0
                        
                        dossier = {
                            "telemetry": {
                                "icao": icao, 
                                "velocity": f"{s[9] or 0} m/s", 
                                "origin_country": origin,
                                "on_ground": str(s[8])
                            }
                        }
                        
                        packet = self._create_czml_packet(
                            entity_id=icao,
                            name=callsign,
                            lon=lon, lat=lat, alt=alt,
                            hex_color="#FFFFFF", # White
                            type_desc="COMMERCIAL",
                            dossier_dict=dossier
                        )
                        packets.append(packet)
                    
                    if packets: self._output_czml("comm_air", packets)
                else:
                    print(f"[L02] ❌ OpenSky HTTP Error (Likely Rate Limit): {r.status_code}")
            except Exception as e:
                print(f"[L02] 💥 Critical Fail: {e}")
    

    async def l03_naval_combatants(self):
        """L03: GLOBAL MARITIME GRID (Real-Time Tracker)"""
        url = "wss://stream.aisstream.io/v0/stream"
        
        # Cache to prevent redundant processing of static data
        if not hasattr(self, 'vessel_info'): self.vessel_info = {}
        
        # Subscription: The entire planet
        sub = {
            "APIKey": self.ais_key, 
            "BoundingBoxes": [[[-90, -180], [90, 180]]]
        }
        
        print("[L03] ⚓ Synchronizing Global Maritime Grid...")
        try:
            async with websockets.connect(url, ping_interval=20, ping_timeout=20) as ws:
                await ws.send(json.dumps(sub))
                
                for _ in range(5):
                    try:
                        raw_msg = await asyncio.wait_for(ws.recv(), timeout=15.0)
                        data = json.loads(raw_msg)
                    except asyncio.TimeoutError:
                        break  # If the socket goes quiet for 15s, exit the loop cleanly
                    except Exception:
                        continue  # If we get a corrupted packet, skip to the next one

                    meta = data.get("MetaData", {})
                    mmsi = meta.get("MMSI")
                    if not mmsi: continue  # Use continue so we don't kill the whole function for one bad ship

                    # 1. UPDATE GLOBAL CACHE (Real-time Ship Type & Name identification)
                    if mmsi not in self.vessel_info:
                        self.vessel_info[mmsi] = {
                            "name": meta.get("ShipName", f"MMSI_{mmsi}").strip(),
                            "type": meta.get("ShipType", 0),
                            "is_unsc": str(mmsi) in getattr(self, 'unsc_targets', [])
                        }

                    # 2. POSITION UPDATE
                    lon, lat = meta.get("longitude"), meta.get("latitude")
                    if lon is not None and lat is not None:
                        ship_data = self.vessel_info[mmsi]
                        
                        # LOGIC: Assign Visual Priority
                        # Military (35) = Green, UN Sanctioned = Red, Standard = Blue
                        color = "#00BFFF" # Default Deep Sky Blue
                        type_label = "VESSEL"

                        if ship_data["type"] == 35:
                            color = "#39FF14" # Tactical Green
                            type_label = "MILITARY"
                        elif ship_data["is_unsc"]:
                            color = "#FF0000" # Warning Red
                            type_label = "DARK_FLEET_TARGET"

                        # Push to Cesium immediately
                        packet = self._create_czml_packet(
                            entity_id=f"VESS_{mmsi}",
                            name=f"{type_label}: {ship_data['name']}",
                            lon=lon, lat=lat, alt=0,
                            hex_color=color,
                            type_desc=type_label,
                            dossier_dict={
                                "mmsi": mmsi,
                                "type_code": ship_data["type"],
                                "flag": meta.get("flag"),
                                "last_seen": meta.get("time_utc")
                            }
                        )
                        
                        # Only print the big catches to avoid terminal spam
                        if ship_data["type"] == 35 or ship_data["is_unsc"]:
                            print(f"[L03] 🎯 {type_label} INTERCEPTED: {ship_data['name']} @ {lat}, {lon}")
                        
                        self._output_czml("global_fleet", [packet])

        except Exception as e:
            print(f"[L03] 💥 Sync Interrupted: {e}. Reconnecting...")

    async def l04_dark_fleet(self):
        """L04: DARK FLEET (UNSC Consolidated List)"""
        # Official stable XML source
        url = "https://scsanctions.un.org/resources/xml/en/consolidated.xml"
        
        print("[L04] 🇺🇳 Syncing UN Security Council Consolidated List...")
        try:
            resp = await self.session.get(url, timeout=60)
            
            if resp.status_code == 200:
                # The UN uses standard XML encoding
                root = ET.fromstring(resp.text)
                
                # UNSC structure: <ENTITIES><ENTITY>...</ENTITY></ENTITIES>
                entities = root.find('ENTITIES')
                packets = []
                
                if entities is not None:
                    for entity in entities.findall('ENTITY'):
                        # Ships are listed under ENTITIES
                        name = entity.findtext('FIRST_NAME', 'UNKNOWN_VESSEL')
                        # The UN hides vessel details in the 'COMMENTS1' field
                        comments = entity.findtext('COMMENTS1', '').lower()
                        
                        # Filter for anything that looks like a ship/vessel
                        if "vessel" in comments or "imo" in comments or "mmsi" in comments:
                            # Simple regex-free IMO extraction
                            imo = "0000000"
                            if "imo:" in comments:
                                # Split text to find the number after 'imo:'
                                imo_part = comments.split("imo:")[1].strip().split()[0]
                                imo = ''.join(filter(str.isdigit, imo_part))

                            dossier = {
                                "intel": {
                                    "imo": imo,
                                    "un_ref": entity.findtext('DATAID', 'N/A'),
                                    "source": "UNSC_CONSOLIDATED"
                                },
                                "strategic_summary": f"UN-Sanctioned Asset: {name}. Mapped via Security Council Resolution."
                            }

                            packet = self._create_czml_packet(
                                entity_id=f"UNSC_{imo}",
                                name=f"UN_TARGET: {name}",
                                lon=0, lat=0, alt=0,
                                hex_color="#0055AA", # UN Blue
                                type_desc="SANCTIONED_HULL",
                                dossier_dict=dossier
                            )
                            packets.append(packet)
                            
                            if len(packets) >= 40: break

                if packets: 
                    self._output_czml("dark_fleet", packets)
                    print(f"[L04] ✔️ {len(packets)} UN targets synced and ready.")
            else:
                print(f"[L04] ❌ UN Server Error: {resp.status_code}")

        except Exception as e:
            print(f"[L04] 💥 UN Sync Error: {e}")


    async def l05_spy_sats(self):
        """L05: SPY SATS (CELESTRAK)"""
        # CelesTrak is aggressive with 403s; impersonation is mandatory here
        url = "https://celestrak.org/NORAD/elements/gp.php?GROUP=active&FORMAT=json"
        
        print("[L05] 🛰️ Refreshing Orbital Reconnaissance...")
        try:
            # curl_cffi's impersonate="chrome120" makes this look like a browser
            resp = await self.session.get(url, timeout=30)
            
            if resp.status_code == 200:
                sats = resp.json()
                packets = []
                for sat in sats[:10]:
                    name = sat.get('OBJECT_NAME', 'CLASSIFIED')
                    norad_id = sat.get('NORAD_CAT_ID')
                    
                    dossier = {
                        "telemetry": {"norad_id": norad_id, "epoch": sat.get('EPOCH')},
                        "strategic_summary": f"Active track: {sat.get('OBJECT_TYPE')}."
                    }
                    # Satellites default to high altitude
                    packet = self._create_czml_packet(norad_id, name, 0, 0, 500000, "#7F00FF", "ORBITAL_ASSET", dossier)
                    packets.append(packet)
                
                if packets: self._output_czml("spy_sat", packets)
                print(f"[L05] ✔️ {len(sats)} orbital tracks updated.")
            else:
                print(f"[L05] ❌ CelesTrak Blocked: {resp.status_code}")
                
        except Exception as e:
            print(f"[L05] 💥 Orbital Sync Fail: {e}")

    # =================================================================
    # GROUP B: STRATEGIC & KINETIC SENSORS (L06-L15)
    # =================================================================

    async def l06_rocket_trajectories(self):
        """
        L06: SPACE LAUNCH INTELLIGENCE (GILU Layer)
        Final robust version with type-checking and simplified parsing.
        """
        url = "https://ll.thespacedevs.com/2.2.0/launch/upcoming"
        params = {
            "limit": "100",
            "mode": "detailed", # Switching back to detailed for more reliable dict nesting
            "format": "json"
        }
        
        async with AsyncSession(impersonate="chrome110") as session:
            print("[L06] 🚀 GILU: Synchronizing Space Launch Manifest...")
            try:
                resp = await session.get(url, params=params, timeout=25)
                
                if resp.status_code != 200:
                    print(f"[L06] ❌ Uplink Error: {resp.status_code}")
                    return  # <--- CHANGED FROM: await asyncio.sleep(60); continue

                data = resp.json()
                
                # Check if data is actually a dict (solves the 'str' attribute error)
                if isinstance(data, str):
                    import json
                    data = json.loads(data)

                launches = data.get('results', [])
                if not isinstance(launches, list):
                    print("[L06] ⚠️ Unexpected data format. Results is not a list.")
                    return  # <--- CHANGED FROM: await asyncio.sleep(60); continue

                packets = []
                for launch in launches:
                    # Detailed mode parsing
                    pad = launch.get('pad', {})
                    lat = pad.get('latitude')
                    lon = pad.get('longitude')
                    
                    if lat is None or lon is None: continue

                    l_id = launch.get('id', 'UNK')
                    mission = launch.get('mission')
                    m_name = mission.get('name') if mission else "CLASSIFIED"
                    provider = launch.get('launch_service_provider', {}).get('name', 'UNKNOWN')

                    # --- THE GRAVITY TURN ARC ---
                    arc_points = []
                    target_alt = 350000 
                    for i in range(101):
                        t = i / 100
                        curr_alt = target_alt * (t ** 2.2) 
                        curr_lat = float(lat) + (t * 0.4)  
                        curr_lon = float(lon) + (t * 2.5)  
                        arc_points.extend([curr_lon, curr_lat, curr_alt])

                    # 1. GROUND PAD
                    packets.append(self._create_czml_packet(
                        entity_id=f"PAD_{l_id}",
                        name=f"PAD: {m_name}",
                        lon=float(lon), lat=float(lat), alt=0,
                        hex_color="#FF4500",
                        type_desc="SPACE_LAUNCH",
                        dossier_dict={
                            "mission": m_name,
                            "provider": provider,
                            "status": launch.get('status', {}).get('name', 'SCHEDULED'),
                            "net": launch.get('net', 'TBD')
                        }
                    ))

                    # 2. PREDICTIVE TRAJECTORY
                    packets.append({
                        "id": f"TRAJECTORY_{l_id}",
                        "name": f"PATH: {m_name}",
                        "polyline": {
                            "positions": {"cartographicDegrees": arc_points},
                            "material": {
                                "polylineGlow": {
                                    "color": {"rgba": [255, 69, 0, 150]},
                                    "glowPower": 0.3
                                }
                            },
                            "width": 6
                        }
                    })

                if packets:
                    self._output_czml("rocket_manifest", packets)
                    print(f"[L06] ✔️ GILU: {len(launches)} Launch Corridors Active.")

            except Exception as e:
                print(f"[L06] 💥 Engine Fault: {type(e).__name__} - {e}")

    async def l07_loitering_munitions(self):
        """
        L07: AI-DRIVEN DRONE SCOUT (OSINT Recon Swarm)
        Personality: Tactical UAV Reconnaissance Specialist.
        Uses DINEI_MOTHER to ingest X, Telegram, and RSS with AI Geocoding.
        """
        from news_engine_vs import DINEI_MOTHER
        from curl_cffi.requests import AsyncSession
        import random
        import math

        # Initialize the high-intensity news engine
        engine = DINEI_MOTHER()
        
        # Define the personality profile for the AI Brain
        scout_profile = {
            "name": "UAV Scout Swarm",
            "goal": "UAV strikes, kamikaze drone impacts, loitering munition sightings, and tactical drone deployments",
            "tags": "drone strike, Shahed-136, Lancet drone, FPV footage, drone wreckage, Bayraktar mission, loitering munition",
            "vibe": "Tactical, focused on kinetic aerial activity and localized impacts"
        }

        print("[L07] 🛰️  Drone Scout: Deploying OSINT recon swarm to scan for kinetic signals...")
        try:
            # 1. FETCH INTEL FROM THE ENGINE
            # The engine handles the X/Telegram scraping and LLM Triage/Geocoding
            async with AsyncSession(impersonate="chrome120") as client:
                results = await engine.fetch_and_process_dinei(client, scout_profile)

            if not results:
                print("[L07] 🚁 Scout: Negative signal. No new drone activity localized.")
                return  # <--- CHANGED FROM: await asyncio.sleep(300); continue

            packets = []
            for intel in results:
                loc = intel.get('location', {})
                lat, lon = loc.get('lat'), loc.get('lon')
                
                if not lat or not lon: continue
                
                # 2. GENERATE TACTICAL ID & CALLSIGN
                scout_id = f"UAV_RECON_{str(intel.get('timestamp', random.random()))[-5:]}"
                intensity = intel.get('intensity', 'MODERATE').upper()
                
                # 3. CREATE DYNAMIC LOITERING ORBIT (Visualizing the 'Scan Zone')
                orbit_points = []
                radius = 0.04  # ~4km radius orbit
                for i in range(0, 361, 30):
                    angle = math.radians(i)
                    p_lat = lat + (radius * math.sin(angle))
                    p_lon = lon + (radius * math.cos(angle))
                    orbit_points.extend([p_lon, p_lat, 6000]) # 6km Altitude

                # 4. BUILD THE BEAUTIFUL DOSSIER
                # We inject the media (X/Telegram photos) and source logos directly
                dossier = {
                    "telemetry": {
                        "callsign": scout_id,
                        "signal_source": intel.get('attribution', {}).get('source'),
                        "intensity": intensity,
                        "localized_at": intel.get('location', {}).get('name'),
                        "intel_url": intel.get('attribution', {}).get('url')
                    },
                    "metadata": {
                        "thumbnail": intel.get('media', {}).get('photo') or self.themes['drone']['img'],
                        "theme_color": "#FF4500" if "HIGH" in intensity else "#FFA500"
                    },
                    "strategic_summary": f"ALERT: {intel.get('title')}. CONTEXT: {intel.get('context')[:200]}..."
                }

                # 5. CZML PACKETS
                # Packet A: The Drone Icon (Scout)
                packets.append(self._create_czml_packet(
                    entity_id=scout_id,
                    name=f"SCOUT: {scout_id}",
                    lon=lon, lat=lat, alt=6000,
                    hex_color=dossier["metadata"]["theme_color"],
                    type_desc="UAV_RECON_UNIT",
                    dossier_dict=dossier
                ))
                
                # Packet B: The Polyline Orbit (The 'Action')
                packets.append({
                    "id": f"ORBIT_{scout_id}",
                    "name": "Loitering Orbit",
                    "polyline": {
                        "positions": {"cartographicDegrees": orbit_points},
                        "material": {
                            "polylineDash": {
                                "color": {"rgba": [255, 165, 0, 100]},
                                "dashLength": 12
                            }
                        },
                        "width": 2,
                        "clampToGround": False
                    }
                })

                # 6. UPLINK TO SUPABASE (Live Database Sync)
                # This pushes to your geo_layers table for frontend display
                #await self._uplink(scout_id, dossier["telemetry"], "drone")

            if packets:
                self._output_czml("drone_scout", packets)
                print(f"[L07] ✔️ Scout Swarm localized {len(results)} kinetic events. Uplink complete.")

        except Exception as e:
            print(f"[L07] 💥 Scout System Failure: {e}")
            

    async def l08_global_investment_tracker(self):
        """
        L08: PLANETARY STRATEGIC INFRASTRUCTURE (BATCH HANDLER)
        Uses ephemeral sessions and segmented quadrants to bypass 
        Overpass API timeouts and 406 errors. Pulls actual lines & labels.
        """
        overpass_url = "https://overpass-api.de/api/interpreter"
        
        # 4 Quadrants to break up the global payload and avoid 504 Timeouts
        quadrants = [
            {"name": "NORTH_WEST", "bbox": "0,-180,90,0"},
            {"name": "NORTH_EAST", "bbox": "0,0,90,180"},
            {"name": "SOUTH_WEST", "bbox": "-90,-180,0,0"},
            {"name": "SOUTH_EAST", "bbox": "-90,0,0,180"}
        ]

        # Explicitly looking for MAJOR pipelines to keep the geometry payload survivable
        tag_query = """
        (
        way["man_made"="pipeline"]["usage"="main"];
        way["man_made"="pipeline"]["substance"~"gas|oil"];
        node["power"="plant"]["capacity"~"^[0-9]{3,4}$"];
        )
        """

        # Vanilla headers (Do not impersonate Chrome here, Overpass blocks it)
        headers = {
            "User-Agent": "VisionSphere_BatchProcessor/1.0",
            "Accept": "application/json"
        }

        print("\n" + "="*50)
        print("[L08] 📡 INITIATING BATCH INFRASTRUCTURE SYNC...")
        print("="*50)
        
        all_infra_packets = []
        
        # 1. THE EPHEMERAL CLIENT: Created and destroyed inside the loop.
        # This prevents the "Zombie Connection" slot-locking that causes 406 errors.
        async with httpx.AsyncClient(timeout=180.0) as client:
            
            for q in quadrants:
                print(f"  [>] Fetching {q['name']} Corridor...")
                
                # 'out geom' grabs the actual line coordinates, not just the center point
                query = f"[out:json][timeout:150][bbox:{q['bbox']}];{tag_query};out geom;"
                
                try:
                    # Standard POST with form-encoded payload
                    resp = await client.post(
                        overpass_url, 
                        data={'data': query}, 
                        headers=headers
                    )

                    if resp.status_code == 200:
                        data = resp.json()
                        elements = data.get('elements', [])
                        
                        mapped_count = 0
                        for el in elements:
                            tags = el.get('tags', {})
                            etype = el.get('type')
                            
                            # METADATA & LABELS
                            name = tags.get('name') or tags.get('operator') or "Strategic Corridor"
                            substance = tags.get('substance', 'Energy').upper()
                            
                            # THEMATIC STYLING
                            color = [0, 255, 255, 255] # Cyan for Gas
                            if "OIL" in substance: 
                                color = [255, 69, 0, 255] # Orange for Oil
                            elif tags.get('power') == 'plant': 
                                color = [138, 43, 226, 255] # Violet for Power
                                
                            packet = {
                                "id": f"OSM_{el['id']}",
                                "name": f"{substance} | {name}",
                                "description": f"Type: {tags.get('man_made', 'Asset')}<br/>Operator: {tags.get('operator', 'Unknown')}",
                                "label": {
                                    # Truncate extremely long names to prevent UI clutter
                                    "text": name if len(name) < 30 and "Strategic" not in name else "",
                                    "font": "10pt Lucida Console",
                                    "fillColor": {"rgba": [255, 255, 255, 255]},
                                    "outlineColor": {"rgba": [0, 0, 0, 255]},
                                    "outlineWidth": 2,
                                    "style": "FILL_AND_OUTLINE",
                                    "verticalOrigin": "BOTTOM",
                                    "pixelOffset": {"cartesian2": [0, -10]},
                                    "disableDepthTestDistance": 1000000 
                                }
                            }

                            # GEOMETRY: LINES (Pipelines)
                            if etype == "way" and "geometry" in el:
                                coords = []
                                for pt in el['geometry']:
                                    coords.extend([pt['lon'], pt['lat'], 0])
                                
                                # Ensure we have at least 2 points (6 floats) to draw a line
                                if len(coords) >= 6: 
                                    packet["polyline"] = {
                                        "positions": {"cartographicDegrees": coords},
                                        "material": {"solidColor": {"color": {"rgba": color}}},
                                        "width": 3,
                                        "clampToGround": True
                                    }
                                    all_infra_packets.append(packet)
                                    mapped_count += 1

                            # GEOMETRY: POINTS (Power Plants)
                            elif etype == "node":
                                lon, lat = el.get('lon'), el.get('lat')
                                if lon and lat:
                                    packet["position"] = {"cartographicDegrees": [lon, lat, 0]}
                                    packet["point"] = {
                                        "pixelSize": 8,
                                        "color": {"rgba": color},
                                        "outlineColor": {"rgba": [0,0,0,255]},
                                        "outlineWidth": 2
                                    }
                                    all_infra_packets.append(packet)
                                    mapped_count += 1

                        print(f"  [+] ✔️ {q['name']}: Mapped {mapped_count} specific assets.")

                    elif resp.status_code == 429:
                        print(f"  [!] ⚠️ Rate limited on {q['name']}. Overpass queue is full.")
                        await asyncio.sleep(45)
                    else:
                        print(f"  [!] ❌ HTTP {resp.status_code} on {q['name']}.")

                except Exception as e:
                    print(f"  [!] 💥 Network/Timeout failure on {q['name']}: {e}")

                # 2. THE SLOT WAITER: Crucial for Overpass. 
                # Give the server 15 seconds to flush our IP from the active queue.
                await asyncio.sleep(15)

        # Output the unified layer to Cesium
        if all_infra_packets:
            self._output_czml("global_infrastructure", all_infra_packets)
            print(f"\n[L08] 🏁 BATCH COMPLETE: {len(all_infra_packets)} assets pushed to VisionSphere.")


    async def l09_battlefield_thermal(self):
        """
        L09: KINETIC IMPACTS (NASA FIRMS)
        Filters global VIIRS thermal data for high-energy anomalies.
        Isolates battlefield strikes and industrial events from routine thermal noise.
        """
        # Directly pull from .env as requested
        api_key = os.getenv("NASA_FIRMS_KEY")
        if not api_key:
            print("[L09] ⚠️ NASA_FIRMS_KEY missing in .env. Thermal tracking disabled.")
            return

        # VIIRS_SNPP_NRT is the gold standard for tactical resolution (375m per pixel)
        url = f"https://firms.modaps.eosdis.nasa.gov/api/area/csv/{api_key}/VIIRS_SNPP_NRT/world/2"
        
        print("\n[L09] 🔥 SWEEPING GLOBAL THERMAL SIGNATURES (NASA FIRMS)...")
        try:
            # Use the existing impersonated session for the fetch
            resp = await self.session.get(url, timeout=45)
            
            if resp.status_code == 200:
                # Using pandas for fast vectorized filtering of the global CSV
                df = pd.read_csv(StringIO(resp.text))
                
                # DYNAMIC TIERING:
                # We set the floor at 20.0 MW. This captures tactical events 
                # without flooding the map with campfire-level noise.
                detections = df[df['frp'] >= 20.0].copy()
                
                packets = []
                for _, row in detections.iterrows():
                    frp = float(row['frp'])
                    lon, lat = float(row['longitude']), float(row['latitude'])
                    
                    # THE INTELLIGENCE CLASSIFICATION MATRIX
                    if frp >= 80.0:
                        status = "CRITICAL_KINETIC"
                        summary = "Massive thermal anomaly. Consistent with high-yield explosion or major industrial event."
                        color = [255, 0, 0, 255] # Pure Red
                        size = 14
                    elif frp >= 40.0:
                        status = "TACTICAL_SPIKE"
                        summary = "High-intensity heat signature. Possible kinetic strike or significant flaring."
                        color = [255, 127, 0, 255] # Orange
                        size = 10
                    else:
                        status = "INDUSTRIAL_HEAT"
                        summary = "Consistent thermal signature. Likely routine flaring or localized industrial fire."
                        color = [255, 255, 0, 255] # Yellow
                        size = 7

                    dossier = {
                        "telemetry": {
                            "radiative_power": f"{frp} MW",
                            "brightness_temp": f"{row.get('bright_ti4', 'N/A')} K",
                            "confidence": row.get('confidence', 'N/A'),
                            "satellite": row.get('satellite', 'VIIRS'),
                            "acquisition": f"{row['acq_date']} {row['acq_time']}Z"
                        },
                        "strategic_summary": summary
                    }

                    # Constructing the CZML packet using the class helper
                    packet = self._create_czml_packet(
                        entity_id=f"FIRMS_{lat}_{lon}_{row['acq_time']}",
                        name=f"THERMAL | {status}",
                        lon=lon, lat=lat, alt=0,
                        hex_color=f"#{color[0]:02x}{color[1]:02x}{color[2]:02x}",
                        type_desc=status,
                        dossier_dict=dossier
                    )
                    
                    # Visual styling for emphasis on the globe
                    packet["point"]["pixelSize"] = size
                    packet["point"]["outlineWidth"] = 2
                    packet["point"]["outlineColor"] = {"rgba": [255, 255, 255, 200]}
                    
                    packets.append(packet)

                print(f"  [+] Identified {len(packets)} strategic thermal anomalies.")
                if packets:
                    self._output_czml("battlefield_thermal", packets)
            
            elif resp.status_code == 403 or resp.status_code == 401:
                print("  [!] NASA FIRMS: Access Denied. Verify your NASA_FIRMS_KEY.")
            else:
                print(f"  [!] NASA FIRMS Error: HTTP {resp.status_code}")

        except Exception as e:
            print(f"  [!] Thermal Engine Failure: {e}")

    async def l10_ew_jamming(self):
        """
        L10: OPENSKY GLOBAL GPS INTERFERENCE DETECTION
        Source: OpenSky Network (Academic/Research Feed)
        Logic: Detects GNSS spoofing by calculating the delta between Barometric (Pressure)
               and Geometric (GPS/WGS84) altitude.
        """
        # OpenSky Global State Vector
        url = "https://opensky-network.org/api/states/all"
        
        # We MUST use a User-Agent or OpenSky will eventually throttle/404 us
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) VisionSphere/1.0"
        }

        print("\n[L10] 📡 SCANNING GLOBAL AIRSPACE VIA OPENSKY...")
        try:
            # OpenSky provides a list of lists (State Vectors)
            resp = await self.session.get(url, headers=headers, timeout=60)
            
            if resp.status_code == 200:
                data = resp.json()
                states = data.get('states', [])
                
                # OpenSky Indices:
                # 5: Longitude, 6: Latitude, 7: Baro Alt (m), 13: Geo Alt (m)
                
                jammed_vessels = []
                for s in states:
                    lon, lat = s[5], s[6]
                    baro_alt, geo_alt = s[7], s[13]

                    if lon and lat and baro_alt:
                        # DETECTION LOGIC:
                        # 1. GPS Signal Loss: Baro Alt exists, but Geo Alt is Null
                        # 2. Spoofing: Gap between Baro and Geo is > 1000 meters (approx 3200ft)
                        if geo_alt is None:
                            jammed_vessels.append({'lat': lat, 'lon': lon, 'type': 'SIGNAL_LOSS'})
                        elif abs(baro_alt - geo_alt) > 1000:
                            jammed_vessels.append({'lat': lat, 'lon': lon, 'type': 'SPOOFING_DETECTED'})

                packets = []
                # Spatial Clustering: Group anomalies into 2.5-degree grid zones
                zones = {}
                for v in jammed_vessels:
                    grid_key = (round(v['lat'] / 2.5) * 2.5, round(v['lon'] / 2.5) * 2.5)
                    if grid_key not in zones: zones[grid_key] = []
                    zones[grid_key].append(v)

                for (lat, lon), instances in zones.items():
                    count = len(instances)
                    # Threshold: At least 5 aircraft reporting issues in one zone
                    if count < 1: continue 

                    # Classify by intensity
                    is_spoof = any(i['type'] == 'SPOOFING_DETECTED' for i in instances)
                    color = "#FF00FF" if is_spoof else "#8B008B" # Magenta for active spoofing
                    
                    dossier = {
                        "ew_analysis": {
                            "active_anomalies": count,
                            "primary_symptom": "GPS_SPOOFING" if is_spoof else "SIGNAL_BLOCKAGE",
                            "source": "OpenSky Network State-Vector Analysis",
                            "confidence": "HIGH" if count > 20 else "MEDIUM"
                        },
                        "strategic_summary": f"Detected significant GNSS delta in this sector. {count} flights affected."
                    }

                    packet = self._create_czml_packet(
                        entity_id=f"EW_ZONE_{lat}_{lon}",
                        name=f"EW | {dossier['ew_analysis']['primary_symptom']}",
                        lon=lon, lat=lat, alt=12000,
                        hex_color=color,
                        type_desc="JAMMING_ZONE",
                        dossier_dict=dossier
                    )
                    
                    # Add the "Interference Bubble" to the Cesium frontend
                    packet["ellipsoid"] = {
                        "radii": {"cartesian": [150000, 150000, 150000]}, # 150km radius
                        "material": {"solidColor": {"color": {"rgba": [255, 0, 255, 35]}}},
                        "outline": True,
                        "outlineColor": {"rgba": [255, 0, 255, 150]}
                    }
                    packets.append(packet)

                print(f"  [+] Identified {len(packets)} active GPS Jamming/Spoofing zones.")
                if packets:
                    self._output_czml("ew_jamming", packets)

            else:
                print(f"  [!] OpenSky Source Error: {resp.status_code}")

        except Exception as e:
            print(f"  [!] L10 OpenSky Failure: {e}")


    def _execute_network_call(self, url, params):
        """ Helper to isolate the exact moment of the network hang """
        print(f"[DEBUG 4] Inside Thread: Initializing curl_cffi.get...")
        try:
            # We perform the GET here
            r = requests.get(
                url, 
                params=params, 
                impersonate="chrome110", 
                timeout=10, 
                verify=False
            )
            print(f"[DEBUG 5] Inside Thread: Network returned status {r.status_code}")
            return r
        except Exception as network_e:
            print(f"[DEBUG 6] Inside Thread: Network call exploded: {network_e}")
            return None

    async def l11_global_outages(self):
        """
        L11: GLOBAL OUTAGES - DIAGNOSTIC OVERDRIVE
        Scenario: Scanning a mirror that frequently drops packets at the SNI/Handshake level.
        """
        url = "https://api.ioda.inetintel.cc.gatech.edu/v2/outages/alerts"
        
        print(f"\n--- [DEBUG START: {datetime.now().strftime('%H:%M:%S')}] ---")
        now = int(time.time())
        params = {"from": str(now - 86400), "until": str(now), "status": "active", "limit": "150"}

        print(f"[DEBUG 1] Parameters initialized: {params}")
        print(f"[DEBUG 2] Entering 'asyncio.wait_for' watchdog wrapper...")

        try:
            print(f"[DEBUG 3] Launching 'asyncio.to_thread' for curl_cffi...")
            
            # THE WATCHDOG: 15 second hard limit
            resp = await asyncio.wait_for(
                asyncio.to_thread(
                    self._execute_network_call, url, params
                ),
                timeout=20 
            )

            print(f"[DEBUG 7] Watchdog passed. Status: {resp.status_code if resp else 'No Resp'}")

            if resp and resp.status_code == 200:
                print(f"[DEBUG 8] Parsing JSON data...")
                data = resp.json()
                alerts = data.get('data', [])
                print(f"[DEBUG 9] Alerts found: {len(alerts)}")

                packets = []
                for alert in alerts:
                    # 1. Access the entity block
                    entity = alert.get('entity', {})
                    # 2. Access the attributes block
                    attrs = entity.get('attrs', {})
                    
                    # 3. Grab the country_code (e.g., "TH" or "TT")
                    code = attrs.get('country_code')
                    
                    # Fallback: If it's a direct country-type alert, it might just be 'code'
                    if not code:
                        code = entity.get('code') if entity.get('type') == 'country' else None

                    if not code:
                        continue

                    code = str(code).upper()
                    coords = self.ISO_MAP.get(code)
                    
                    if coords:
                        print(f"[L11] 📍 Mapping Outage: {code} ({entity.get('name')})")
                        packet = self._create_czml_packet(
                            entity_id=f"IODA_{code}_{int(time.time())}_{alert.get('value')}",
                            name=f"OUTAGE | {code}",
                            lon=coords[1], lat=coords[0], alt=0,
                            hex_color="#FF4500",
                            type_desc="INFRA_OUTAGE",
                            dossier_dict={
                                "location": entity.get('name'),
                                "severity": alert.get('level'),
                                "source": alert.get('datasource')
                            }
                        )
                        packets.append(packet)

                print(f"[DEBUG 10.Final] Loop finished. Total packets created: {len(packets)}")

                if packets:
                    print(f"[DEBUG 11] Writing to CZML file: global_blackouts.czml")
                    self._output_czml("global_blackouts", packets)
                    print(f"[DEBUG 12] Write complete.")
            else:
                print(f"[DEBUG 13] Mirror Error or Null Resp: {resp.status_code if resp else 'NULL'}")

        except asyncio.TimeoutError:
            print("[DEBUG STUCK] WATCHDOG TRIGGERED: The network call froze the thread for >20s.")
        except Exception as e:
            print(f"[DEBUG CRASH] Logic Error: {e}")

        print(f"--- [DEBUG END: {datetime.now().strftime('%H:%M:%S')}] Sleeping... ---\n")

    async def l12_seismic_activity(self):
        """L12: GLOBAL SEISMIC ACTIVITY (USGS MAG 4.0+)"""
        url = "https://earthquake.usgs.gov/earthquakes/feed/v1.0/summary/all_day.geojson"

        print(f"[L12] ⚡ {datetime.now().strftime('%H:%M:%S')} Scanning Global Seismic Activity...")
        try:
            resp = await self.session.get(url, timeout=20)
            
            if resp.status_code == 200:
                data = resp.json()
                
                # Filter for Mag 4.0+ to keep the dashboard focused on real impact
                significant = [f for f in data.get('features', []) 
                                if f['properties'].get('mag') and f['properties']['mag'] >= 4.0]
                
                packets = []
                for f in significant:
                    props = f['properties']
                    # GeoJSON natively holds coordinates as [longitude, latitude, depth]
                    geom = f['geometry']['coordinates'] 
                    lon, lat, depth = geom[0], geom[1], geom[2]
                    mag = props['mag']
                    
                    # Dynamic visual sizing: Mag 4 is ~64km, Mag 7 is ~340km radius
                    radius = (mag ** 3) * 1000 
                    
                    packet = self._create_czml_packet(
                        entity_id=f"QUAKE_{f['id']}",
                        name=f"MAG {mag} | {props.get('place', 'Unknown')}",
                        lon=lon, lat=lat, alt=0,
                        hex_color="#FFA500", # Orange to differentiate from the Red cyber attacks
                        type_desc="SEISMIC_EVENT",
                        dossier_dict={
                            "magnitude": mag,
                            "depth_km": depth,
                            "time": datetime.fromtimestamp(props['time']/1000).strftime('%Y-%m-%d %H:%M:%S'),
                            "source": "USGS Global Feed"
                        }
                    )

                    # Visual: Orange Seismic Ripple
                    packet["ellipsoid"] = {
                        "radii": {"cartesian": [radius, radius, 10]},
                        "material": {"solidColor": {"color": {"rgba": [255, 165, 0, 70]}}},
                        "outline": True,
                        "outlineColor": {"rgba": [255, 140, 0, 255]},
                        "outlineWidth": 3
                    }
                    packets.append(packet)

                if packets:
                    self._output_czml("global_seismic", packets)
                    print(f"  [+] Synced {len(packets)} Mag 4.0+ earthquakes to map.")
            else:
                print(f"  [!] USGS API Error: {resp.status_code}")

        except Exception as e:
            print(f"  [!] L12 Crash: {e}")
            
            
    async def l13_strategic_radiation(self):
        """
        L13: GLOBAL RADIATION SCANNER (Safecast)
        Visuals: Pulsing Green "Shockwave" Dots. No filtering.
        """
        url = "https://api.safecast.org/measurements.json"
        
        print(f"[L13] ☢️ SYNCING ALL GLOBAL DOSIMETERS...")
        
        try:
            resp = await asyncio.to_thread(
                requests.get, 
                url, 
                impersonate="chrome110", 
                timeout=15, 
                verify=False
            )

            if resp and resp.status_code == 200:
                measurements = resp.json()
                packets = []

                for m in measurements:
                    val = m.get('value', 0)
                    lat = m.get('latitude')
                    lon = m.get('longitude')
                    
                    if not lat or not lon:
                        continue

                    # Calculate "Radiant Extent"
                    # We scale the dot size based on the value so higher rads "shake" more space
                    # Background (40) = ~12px. Spike (400) = ~48px.
                    base_size = 8 + (val / 10) 
                    shockwave_size = base_size * 1.5

                    packet = self._create_czml_packet(
                        entity_id=f"RAD_LIVE_{m.get('id')}",
                        name=f"RAD: {val} {m.get('unit', 'cpm')}",
                        lon=lon, lat=lat, alt=0,
                        hex_color="#00FF00", # Pure Toxic Green
                        type_desc="DOSIMETER_NODE",
                        dossier_dict={
                            "reading": f"{val} {m.get('unit')}",
                            "timestamp": m.get('captured_at'),
                            "status": "ACTIVE_STREAM"
                        }
                    )

                    # OVERRIDE POINT STYLE FOR "SHOCKWAVE" EFFECT
                    # This uses a glowing center with a massive semi-transparent outline
                    packet["point"] = {
                        "color": {"rgba": [0, 255, 0, 255]},
                        "pixelSize": base_size,
                        "outlineColor": {"rgba": [0, 255, 0, 100]}, # Faded green "aura"
                        "outlineWidth": shockwave_size, # The "Shockwave"
                        "disableDepthTestDistance": 1.2e+7 # Keep it visible through the globe if needed
                    }
                    
                    packets.append(packet)

                if packets:
                    self._output_czml("radiation_spikes", packets)
                    print(f"  [+] SUCCESS: {len(packets)} radiation nodes mapped to globe.")
            
        except Exception as e:
            print(f"  [🛑] RAD SCANNER ERROR: {e}")

    async def l14_airspace_closures(self):
        """
        L14: GLOBAL AIRSPACE HAZARDS (AWC API)
        Handles the specific empty-dict artifacts and coordinate parsing.
        """
        url = "https://aviationweather.gov/api/data/isigmet?format=json"
        
        hazard_styles = {
            "TS": {"color": [255, 165, 0, 100], "label": "THUNDERSTORM"},
            "TURB": {"color": [255, 255, 0, 100], "label": "TURBULENCE"},
            "ICE": {"color": [0, 191, 255, 100], "label": "ICING"},
            "VA": {"color": [255, 0, 0, 120], "label": "VOLCANIC ASH"},
            "MTW": {"color": [147, 112, 219, 100], "label": "MTN WAVE"}
        }

        print("[L14] 🛑 FETCHING GLOBAL SIGMETS...")
        try:
            resp = await self.session.get(url, timeout=30)
            if resp.status_code == 200:
                data = resp.json()
                packets = []

                # Ensure we have a list to iterate through
                if not isinstance(data, list):
                    return

                for item in data:
                    # FIREWALL: Skip empty dicts {} or non-dict items
                    if not item or not isinstance(item, dict):
                        continue
                    
                    # Verify essential keys exist before proceeding
                    if "coords" not in item or "hazard" not in item:
                        continue
                        
                    hazard_key = item.get('hazard', 'UNK')
                    style = hazard_styles.get(hazard_key, {"color": [200, 200, 200, 100], "label": "UNKNOWN"})
                    
                    raw_coords = item.get('coords', [])
                    if not raw_coords or not isinstance(raw_coords, list):
                        continue
                        
                    czml_coords = []
                    valid_pt = None
                    
                    for pt in raw_coords:
                        # Verify coordinate point is a dict with lat/lon
                        if isinstance(pt, dict) and 'lon' in pt and 'lat' in pt:
                            if valid_pt is None: valid_pt = pt
                            czml_coords.extend([pt['lon'], pt['lat'], 0])

                    if not czml_coords:
                        continue

                    # Altitude processing with defaults
                    top_ft = item.get('top') if item.get('top') else 35000
                    base_ft = item.get('base') if item.get('base') else 0
                    top_m = float(top_ft) * 0.3048
                    base_m = float(base_ft) * 0.3048

                    packet = {
                        "id": f"SIGMET_{item.get('seriesId', 'UNK')}_{item.get('icaoId', 'UNK')}",
                        "name": f"HAZARD: {style['label']}",
                        "polygon": {
                            "positions": {"cartographicDegrees": czml_coords},
                            "material": {"solidColor": {"color": {"rgba": style['color']}}},
                            "height": base_m,
                            "extrudedHeight": top_m,
                            "outline": True,
                            "outlineColor": {"rgba": [255, 255, 255, 255]}
                        },
                        "description": f"FIR: {item.get('firName')}\nHazard: {hazard_key}\nAltitude: {base_ft}-{top_ft} FT"
                    }
                    packets.append(packet)

                if packets:
                    # Assuming your engine has a CZML output handler
                    print(f"  [+] SUCCESS: {len(packets)} sigmets mapped.")
                    self._output_czml("airspace_hazards", packets)
        
        except Exception as e:
            print(f"  [🛑] L14 PARSER ERROR: {e}")

    def _get_rgba_from_gdp(self, gdp):
        """Returns a color scale based on GDP per capita."""
        if gdp >= 50000: return [0, 191, 255, 150]    # Deep Sky Blue
        if gdp >= 25000: return [50, 205, 50, 150]   # Lime Green
        if gdp >= 10000: return [255, 255, 0, 150]   # Yellow
        if gdp >= 2500:  return [255, 140, 0, 150]   # Dark Orange
        return [255, 0, 0, 150]                      # Red

    def _flatten_coords(self, coords_list):
        """High-speed coordinate flattening for CZML Cartesian3."""
        # GeoJSON is [lon, lat], CZML expects [lon, lat, alt]
        flat = []
        for pt in coords_list:
            flat.extend([pt[0], pt[1], 0])
        return flat

    async def l15_global_wealth_topography(self):
        wb_url = "https://api.worldbank.org/v2/country/all/indicator/NY.GDP.PCAP.CD?format=json&per_page=300&mrv=1"
        geojson_url = "https://raw.githubusercontent.com/datasets/geo-countries/master/data/countries.geojson"
        
        cached_geo_data = None

        print("\n[L15] 🗺️ INITIATING GLOBAL WEALTH TOPOGRAPHY SCAN...")
        try:
            if not cached_geo_data:
                print("  [>] Downloading Global Boundaries...")
                resp = await self.session.get(geojson_url, timeout=60)
                cached_geo_data = resp.json()
                print(f"  [+] Loaded {len(cached_geo_data['features'])} geographical features.")

            print("  [>] Fetching World Bank Economic Indicators...")
            wb_resp = await self.session.get(wb_url, timeout=30)
            wb_raw = wb_resp.json()
            
            if len(wb_raw) < 2:
                print("  [🛑] World Bank returned empty data array.")
                return  # <--- CHANGED FROM: continue
            
            # FIX: Normalize keys to UPPERCASE for reliable matching
            wealth_map = {}
            for item in wb_raw[1]:
                iso = item.get('countryiso3code')
                val = item.get('value')
                if iso and val:
                    wealth_map[iso.upper()] = val # Store as 'USA' instead of 'usa'

            print(f"  [+] Economic data synced for {len(wealth_map)} nations.")

            packets = []
            for feature in cached_geo_data.get('features', []):
                props = feature.get('properties', {})
                
                # FIX: Search for the specific key in your JSON file
                # We check the standard ones AND your specific "ISO3166-1-Alpha-3" key
                iso_a3 = (
                    props.get('ISO3166-1-Alpha-3') or 
                    props.get('ISO_A3') or 
                    props.get('iso_a3') or 
                    ""
                ).upper()
                
                name = props.get('name') or props.get('ADMIN', 'Unknown')
                gdp = wealth_map.get(iso_a3)

                if not gdp:
                    # Log one sample to console so you know the matching is working
                    if iso_a3 == "IDN":
                        print(f"  [?] Found Indonesia ({iso_a3}) but no GDP data found in WB map.")
                    continue

                # DEBUG: Success Heartbeat
                if iso_a3 == "IDN":
                    print(f"  [+] Match Success: {name} ({iso_a3}) -> ${gdp:,.0f}")

                rgba = self._get_rgba_from_gdp(gdp)
                geom = feature.get('geometry')
                
                if not geom or 'coordinates' not in geom:
                    continue

                if geom['type'] == 'Polygon':
                    coords = self._flatten_coords(geom['coordinates'][0])
                    packets.append(self._build_country_packet(iso_a3, name, coords, rgba, gdp))

                elif geom['type'] == 'MultiPolygon':
                    for idx, poly in enumerate(geom['coordinates']):
                        # MultiPolygons in your data are nested: [ [ [coord, coord] ] ]
                        # We take the first element of the polygon (the outer ring)
                        if len(poly[0]) < 3: continue 
                        coords = self._flatten_coords(poly[0])
                        packets.append(self._build_country_packet(f"{iso_a3}_{idx}", name, coords, rgba, gdp))

            if packets:
                print(f"  [>] Pushing {len(packets)} CZML packets...")
                self._output_czml("wealth_topography", packets)
                print(f"  [🏁] L15 COMPLETE.")
            else:
                # Logic failsafe
                #sample_wb = list(wealth_map.keys())[:3]
                #sample_geo = [f.get('properties', {}).get('ISO3166-1-Alpha-3') for f in cached_geo_data['features'][:3]]
                #print(f"  [❌] ERROR: Zero packets. Sample WB keys: {sample_wb}. Sample GeoJSON keys: {sample_geo}")
                pass

        except Exception as e:
            print(f"  [🛑] L15 ERROR: {e}")
            

    def _build_country_packet(self, entry_id, name, coords, rgba, gdp):
        """Helper to structure the individual CZML entity."""
        return {
            "id": entry_id,
            "name": f"{name} (GDP: ${gdp:,.0f})",
            "polygon": {
                "positions": {
                    "cartographicDegrees": coords
                },
                "material": {
                    "solidColor": {
                        "color": {
                            "rgba": rgba
                        }
                    }
                },
                "height": 0,
                "extrudedHeight": min(gdp / 10, 500000), # Visual extrusion based on wealth
                "outline": True,
                "outlineColor": {"rgba": [255, 255, 255, 50]}
            }
        }

    async def l16_submarine_cables(self):
        """
        L16: SUBMARINE CABLE NETWORK (TELEGEOGRAPHY)
        Visuals: Glowing Cyan underwater data lines.
        """
        url = "https://www.submarinecablemap.com/api/v3/cable/cable-geo.json"
        
        print("\n[L16] 🌊 SCANNING GLOBAL SUBMARINE CABLES...")
        try:
            async with httpx.AsyncClient(timeout=60.0) as client:
                r = await client.get(url)
                if r.status_code == 200:
                    data = r.json()
                    features = data.get('features', [])
                    packets = []

                    for feature in features:
                        props = feature.get('properties', {})
                        geom = feature.get('geometry', {})
                        
                        if not geom or geom.get('type') not in ['LineString', 'MultiLineString']:
                            continue

                        cable_id = props.get('id', 'unk')
                        name = props.get('name', 'Submarine Cable')
                        
                        # Handle both single lines and branched cable systems
                        coords_list = geom.get('coordinates', [])
                        lines = coords_list if geom['type'] == 'MultiLineString' else [coords_list]
                        
                        for idx, line in enumerate(lines):
                            czml_coords = []
                            for pt in line:
                                if len(pt) >= 2:
                                    czml_coords.extend([pt[0], pt[1], 0])
                            
                            # Ensure we have at least 2 points to draw a polyline
                            if len(czml_coords) >= 6:
                                packet = self._create_czml_packet(
                                    entity_id=f"CABLE_{cable_id}_{idx}",
                                    name=f"🌐 {name}",
                                    lon=line[0][0], lat=line[0][1], alt=0,
                                    hex_color="#00FFFF", 
                                    type_desc="SUBMARINE_CABLE",
                                    dossier_dict={
                                        "length": props.get('length', 'Unknown'),
                                        "owners": props.get('owners', 'Unknown'),
                                        "rfs": props.get('rfs', 'Unknown') # Ready For Service Year
                                    }
                                )
                                
                                # Replace the default point with a glowing Polyline
                                packet.pop("point", None)
                                packet["polyline"] = {
                                    "positions": {"cartographicDegrees": czml_coords},
                                    "material": {
                                        "polylineGlow": {
                                            "color": {"rgba": [0, 255, 255, 200]},
                                            "glowPower": 0.2
                                        }
                                    },
                                    "width": 2,
                                    "clampToGround": True
                                }
                                packets.append(packet)

                    if packets:
                        self._output_czml("submarine_cables", packets)
                        print(f"  [+] SUCCESS: {len(packets)} oceanic data corridors mapped.")

        except Exception as e:
            print(f"  [🛑] L16 CABLE ERROR: {e}")

    # =================================================================
    def _hex_to_rgba(self, hex_color, alpha=255):
        """Helper to convert #RRGGBB to RGBA array for Cesium styling."""
        hex_color = hex_color.lstrip('#')
        if len(hex_color) == 6:
            return [int(hex_color[i:i+2], 16) for i in (0, 2, 4)] + [alpha]
        return [255, 255, 255, alpha]

    async def _osm_fetch(self, query, layer_id, category, icon="📍", hex_color="#FFFFFF"):
        """Standardized OpenStreetMap fetcher for static infrastructure."""
        overpass_url = "https://overpass-api.de/api/interpreter"
        headers = {
            "User-Agent": "VisionSphere_Strategic_Scanner/2.0",
            "Content-Type": "application/x-www-form-urlencoded"
        }
        
        print(f"\n[{layer_id.upper()}] 🛰️ SCANNING {category.upper()} VIA OVERPASS...")
        
        try:
            # Ephemeral client prevents API bans
            async with httpx.AsyncClient(timeout=240.0) as client:
                r = await client.post(overpass_url, data={'data': query}, headers=headers)
                
                if r.status_code == 200:
                    elements = r.json().get('elements', [])
                    packets = []
                    
                    for el in elements:
                        tags = el.get('tags', {})
                        lon = el.get('lon') or el.get('center', {}).get('lon')
                        lat = el.get('lat') or el.get('center', {}).get('lat')
                        
                        if lon is None or lat is None:
                            continue
                            
                        name = tags.get('name') or tags.get('operator') or f"Strategic {category}"
                        
                        # Filter tags for a clean dossier
                        dossier = {k: v for k, v in tags.items() if k in ['operator', 'military', 'aeroway', 'generator:source', 'industrial', 'natural', 'telecom', 'building']}

                        packet = self._create_czml_packet(
                            entity_id=f"OSM_{el['id']}",
                            name=f"{icon} {name}",
                            lon=lon, lat=lat, alt=0,
                            hex_color=hex_color,
                            type_desc=category.upper(),
                            dossier_dict=dossier
                        )
                        
                        # Enhance the visual Point
                        packet["point"] = {
                            "pixelSize": 8,
                            "color": {"rgba": self._hex_to_rgba(hex_color, 255)},
                            "outlineColor": {"rgba": [0,0,0,255]},
                            "outlineWidth": 2
                        }
                        
                        # Apply floating label if it has an official name
                        if tags.get('name'):
                            packet["label"] = {
                                "text": name if len(name) < 25 else "",
                                "font": "10pt monospace",
                                "fillColor": {"rgba": [255,255,255,255]},
                                "outlineColor": {"rgba": [0,0,0,255]},
                                "outlineWidth": 2,
                                "style": "FILL_AND_OUTLINE",
                                "verticalOrigin": "BOTTOM",
                                "pixelOffset": {"cartesian2": [0, -12]},
                                "disableDepthTestDistance": 1000000 
                            }
                            
                        packets.append(packet)

                    if packets:
                        self._output_czml(layer_id, packets)
                        print(f"  [+] SUCCESS: {len(packets)} {category} locations mapped.")
                    else:
                        print(f"  [!] Scan complete, no elements found.")
                        
                elif r.status_code == 429:
                    print(f"  [!] ⚠️ Rate limited. Wait before retry.")
                else:
                    print(f"  [!] ❌ HTTP {r.status_code}")

        except Exception as e:
            print(f"  [🛑] {layer_id.upper()} NETWORK ERROR: {e}")

    # =================================================================
    # GROUP C: STRATEGIC INFRASTRUCTURE ENGINE 
    # =================================================================

    async def l17_strategic_airbases(self):
        q = '[out:json][timeout:180];way["military"="airfield"]["aeroway"="runway"];out center;'
        await self._osm_fetch(q, "airbases", "Military Airbase", icon="🛩️", hex_color="#FF4500") # Orange Red

    async def l18_nuclear_enrichment(self):
        q = '[out:json][timeout:180];way["generator:source"="nuclear"];out center;'
        await self._osm_fetch(q, "nuclear", "Nuclear Facility", icon="☢️", hex_color="#FF1493") # Deep Pink

    async def l19_rare_earth_mining(self):
        q = '[out:json][timeout:180];way["industrial"="mine"];out center;'
        await self._osm_fetch(q, "mining", "Industrial Mine", icon="⛏️", hex_color="#8B4513") # Saddle Brown

    async def l20_maritime_chokepoints(self):
        q = '[out:json][timeout:180];node["natural"="strait"];out;'
        await self._osm_fetch(q, "chokepoints", "Maritime Strait", icon="🚢", hex_color="#1E90FF") # Dodger Blue

    async def l21_major_datacenters(self):
        q = '[out:json][timeout:180];way["telecom"="data_center"];out center;'
        await self._osm_fetch(q, "datacenters", "Data Center", icon="💾", hex_color="#00FF00") # Lime Green

    async def l26_global_hangars(self):
        q = '[out:json][timeout:180];way["building"="hangar"];out center;'
        await self._osm_fetch(q, "hangars", "Strategic Hangar", icon="🏭", hex_color="#808080") # Gray

    # =================================================================
    # GROUP D: BORDERS, SIGNALS & ECONOMICS (22-25, 32-33)
    # =================================================================

    async def l22_sanctions_embargos(self):
        """
        Deep-scans OFAC Recent Actions across multiple pages and resolves 
        text-based designations into geospatial coordinates.
        """
        base_url = "https://ofac.treasury.gov/recent-actions"
        # We'll scan the first 5 pages to get a solid historical baseline
        max_pages = 5 
        
        print("\n[L22] ⚖️ STARTING DEEP-SCAN SANCTIONS MONITOR...")

        all_packets = []
        try:
            for page in range(max_pages):
                url = f"{base_url}?page={page}"
                print(f"  [>] Scraping OFAC Page {page}...")
                
                resp = await self.session.get(url, timeout=30)
                if resp.status_code != 200:
                    break # Stop if we hit a wall

                soup = BeautifulSoup(resp.text, 'html.parser')
                rows = soup.find_all('div', class_='views-row')

                for row in rows:
                    title_elem = row.find('h3') or row.find('a')
                    if not title_elem: continue
                    
                    title = title_elem.get_text(strip=True)
                    summary_elem = row.find('div', class_='field--name-field-description')
                    summary = summary_elem.get_text(strip=True) if summary_elem else ""
                    
                    # Combine text for analysis
                    blob = f"{title} {summary}".upper()
                    
                    # RESOLVE GEOGRAPHY
                    geo_node = self._resolve_geospatial_node(blob)
                    if not geo_node:
                        continue # Skip if no country match found

                    # PACKET CONSTRUCTION
                    packet = self._build_sanction_packet(geo_node, title, summary)
                    all_packets.append(packet)

            if all_packets:
                print(f"  [+] Deep-scan complete: {len(all_packets)} sanctioned nodes mapped.")
                self._output_czml("sanctions_embargos", all_packets)
            
        except Exception as e:
            print(f"  [🛑] L22 DEEP-SCAN ERROR: {e}")


    def _resolve_geospatial_node(self, text):
        """
        A 'Free' Local Geocoder. 
        Matches text against a robust set of geopolitical aliases.
        """
        # This expands on your ISO_MAP. It maps text keywords to your map keys.
        geo_directory = {
            "RUSSIA": "RU", "RUSSIAN": "RU", "MOSCOW": "RU",
            "IRAN": "IR", "TEHRAN": "IR", "ISLAMIC REPUBLIC": "IR",
            "NORTH KOREA": "KP", "DPRK": "KP", "PYONGYANG": "KP",
            "CHINA": "CN", "CHINESE": "CN", "BEIJING": "CN",
            "SYRIA": "SY", "DAMASCUS": "SY",
            "VENEZUELA": "VE", "CARACAS": "VE",
            "BELARUS": "BY", "MINSK": "BY",
            "YEMEN": "YE", "HOUTHI": "YE",
            "SUDAN": "SD", "KHARTOUM": "SD",
            "MYANMAR": "MM", "BURMA": "MM",
            "CUBA": "CU", "HAVANA": "CU",
            "UKRAINE": "UA", "CRIMEA": "UA", "DONETSK": "UA" # Conflict zones
        }

        for keyword, iso in geo_directory.items():
            # Use regex to find whole words only (prevents 'IR' matching 'BIRD')
            if re.search(rf"\b{keyword}\b", text):
                coords = self.ISO_MAP.get(iso)
                if coords:
                    return {"iso": iso, "coords": coords, "name": keyword.title()}
        
        return None

    def _build_sanction_packet(self, geo_node, title, summary):
        """Generates a high-impact tactical CZML entry with status-aware styling."""
        iso = geo_node['iso']
        coords = geo_node['coords']
        full_text = f"{title} {summary}".upper()

        # 1. STATUS DETECTION (Red for danger, Green for delisting)
        is_removal = any(word in full_text for word in ["REMOVAL", "DELISTED", "UNFILTERED"])
        
        if is_removal:
            status_label = "RESTRICTION LIFTED"
            main_rgba = [0, 255, 100, 255]      # Sharp Green
            zone_rgba = [0, 255, 100, 40]       # Ghost Green
        else:
            status_label = "ACTIVE EMBARGO"
            main_rgba = [255, 0, 0, 255]        # Alert Red
            zone_rgba = [255, 0, 0, 40]         # Danger Red

        # 2. COLLISION AVOIDANCE & JITTER
        # Create a unique hash for the ID so removals and additions don't overwrite
        entry_hash = hashlib.md5(title.encode()).hexdigest()[:8]
        
        # Slight coordinate jitter (approx 10-20km) so clusters are visible
        jitter_lat = coords[0] + (random.uniform(-0.1, 0.1))
        jitter_lon = coords[1] + (random.uniform(-0.1, 0.1))

        return {
            "id": f"sanction_{iso}_{entry_hash}",
            "name": f"{status_label}: {geo_node['name']}",
            "description": f"<div style='color:white;'><b>Action:</b> {title}<br/><br/>{summary}</div>",
            "position": {
                "cartographicDegrees": [jitter_lon, jitter_lat, 2000] # Lon, Lat, Alt
            },
            "point": {
                "pixelSize": 12,
                "color": {"rgba": main_rgba},
                "outlineColor": {"rgba": [255, 255, 255, 255]},
                "outlineWidth": 2
            },
            # 3. TACTICAL VISUALIZATION
            "cylinder": {
                "length": 1200000.0 if not is_removal else 600000.0, # Shorter for removals
                "topRadius": 150000.0,
                "bottomRadius": 0.0,
                "material": {
                    "solidColor": {
                        "color": {"rgba": zone_rgba}
                    }
                },
                "outline": True,
                "outlineColor": {"rgba": main_rgba}
            }
        }

    def get_coords(self, iso_or_name):
        """
        Lookup coordinates for a given ISO3 code or country name.
        Now correctly accepts 'self' as the first positional argument.
        """
        # Assuming GLOBAL_MAP is defined at the class level or as a global
        # If it's a class variable, use self.GLOBAL_MAP.get(...)
        coords = GLOBAL_MAP.get(iso_or_name, [0.0, 0.0])
        return coords

    # --- THE CZML ECOSYSTEM BUILDERS ---

    def _build_trade_packet(self, iso, name, value):
        """
        Cyan Pulse: Scaling point based on Export Volume.
        Integrated into the class ecosystem via the 'self' reference.
        """
        # Assuming get_coords is a method of your class
        # If get_coords is a standalone function, keep it as: lat, lon = get_coords(iso)
        lat, lon = self.get_coords(iso)
        
        return {
            "id": f"trade_{iso}",
            "name": f"TRADE: {name}",
            "description": f"Exports: ${value:,.0f}",
            "position": {"cartographicDegrees": [lon, lat, 0]},
            "point": {
                "pixelSize": 10 + (value / 5e10), # Scale based on billions
                "color": {"rgba": [0, 255, 255, 150]},
                "outlineColor": {"rgba": [255, 255, 255, 255]},
                "outlineWidth": 1
            }
        }

    def _build_migration_packet(self, origin_name, dest_name, count):
        """Amber Arcs: Geodesic lines showing human movement."""
        # Simplified: Normally you'd lookup coordinates for these names
        o_lat, o_lon = self.get_coords(origin_name[:3].upper()) 
        d_lat, d_lon = self.get_coords(dest_name[:3].upper())
        
        if o_lat == 0 or d_lat == 0: return None # Skip if unknown
        
        return {
            "id": f"mig_{hash(origin_name + dest_name)}",
            "name": f"FLOW: {origin_name} -> {dest_name}",
            "polyline": {
                "positions": {
                    "cartographicDegrees": [
                        o_lon, o_lat, 0,
                        (o_lon + d_lon) / 2, (o_lat + d_lat) / 2, 500000, # Mid-point lift
                        d_lon, d_lat, 0
                    ]
                },
                "material": {
                    "polylineArrow": {"color": {"rgba": [255, 191, 0, 200]}} # Amber
                },
                "width": 5 + (count / 100000),
                "arcType": "GEODESIC"
            }
        }

    def _build_friction_packet(self, port_name, coords, wait_time):
        """Red Needle: Vertical spikes indicating border wait time."""
        lat, lon = coords
        return {
            "id": f"fric_{hash(port_name)}",
            "name": f"CHOKEPOINT: {port_name}",
            "polyline": {
                "positions": {
                    "cartographicDegrees": [
                        lon, lat, 0,
                        lon, lat, wait_time * 4000 # 1 min = 4km spike
                    ]
                },
                "material": {"solidColor": {"color": {"rgba": [255, 0, 0, 255]}}},
                "width": 12
            }
        }

    async def l23_atmospheric_friction(self):
        """
        L23: ATMOSPHERIC FRICTION
        Debug version with strict timeouts and status prints.
        """
        import httpx
        import asyncio

        MONITOR_ZONES = [
            # --- The Major Canals ---
            {"name": "Suez Canal", "lat": 29.9, "lon": 32.5},
            {"name": "Panama Canal", "lat": 9.1, "lon": -79.9},
            {"name": "Kiel Canal", "lat": 54.2, "lon": 9.5},

            # --- Strategic Straits (The Chokepoints) ---
            {"name": "Strait of Hormuz", "lat": 26.6, "lon": 56.5},         # Oil transit
            {"name": "Strait of Malacca", "lat": 1.4, "lon": 102.9},       # Asia-Europe link
            {"name": "Bab-el-Mandeb", "lat": 12.6, "lon": 43.3},           # Red Sea entrance
            {"name": "Strait of Gibraltar", "lat": 35.9, "lon": -5.5},     # Mediterranean entrance
            {"name": "Bosporus Strait", "lat": 41.2, "lon": 29.1},         # Black Sea exit
            {"name": "English Channel", "lat": 50.6, "lon": 1.1},          # Dover Strait friction
            {"name": "Taiwan Strait", "lat": 24.4, "lon": 119.3},          # Geopolitical friction
            {"name": "Tsushima Strait", "lat": 34.4, "lon": 129.5},        # Sea of Japan access

            # --- Southern & Northern Cape Routes ---
            {"name": "Cape of Good Hope", "lat": -34.4, "lon": 18.5},      # Southern Africa bypass
            {"name": "Strait of Magellan", "lat": -52.6, "lon": -70.7},     # Southern America bypass
            {"name": "Bering Strait", "lat": 66.0, "lon": -168.9},         # Arctic gateway

            # --- High-Intensity Logistics Hubs ---
            {"name": "Port of Rotterdam", "lat": 51.9, "lon": 4.1},        # Europe's lung
            {"name": "Port of Singapore", "lat": 1.2, "lon": 103.8},       # Global transshipment
            {"name": "Port of Shanghai", "lat": 30.6, "lon": 122.1}        # World's busiest port
        ]

        print(f"[L23] 🌩️ Cycle Start: Monitoring {len(MONITOR_ZONES)} zones...")
        packets = []
        
        # Use a single client with a set timeout to prevent hanging
        async with httpx.AsyncClient(timeout=10.0) as client:
            for zone in MONITOR_ZONES:
                try:
                    print(f"[L23] --> Fetching: {zone['name']}...")
                    
                    url = "https://api.open-meteo.com/v1/forecast"
                    params = {
                        "latitude": zone['lat'],
                        "longitude": zone['lon'],
                        "current": "weather_code,wind_speed_10m",
                        "timezone": "auto"
                    }
                    
                    response = await client.get(url, params=params)
                    
                    if response.status_code == 200:
                        data = response.json().get('current', {})
                        wmo = data.get('weather_code', 0)
                        print(f"[L23] OK: {zone['name']} (Code {wmo})")
                        
                        # Only generate packet if it's "Friction" weather (>= 51)
                        if wmo >= 0:
                            p_id = f"WX_{zone['name'].replace(' ', '_')}"
                            packets.append({
                                "id": p_id,
                                "name": f"FRICTION: {zone['name']}",
                                "position": {"cartographicDegrees": [zone['lon'], zone['lat'], 0]},
                                "point": {
                                    "pixelSize": 15,
                                    "color": {"rgba": [0, 255, 255, 200]},
                                    "outlineColor": {"rgba": [255, 255, 255, 255]},
                                    "outlineWidth": 2
                                },
                                "description": f"WMO Weather Code: {wmo}\nWind Speed: {data.get('wind_speed_10m')} km/h"
                            })
                    else:
                        print(f"[L23] ⚠️ API Error {response.status_code} at {zone['name']}")

                except httpx.ConnectTimeout:
                    print(f"[L23] ❌ Connection Timeout at {zone['name']}. Network is blocking Python.")
                except Exception as e:
                    print(f"[L23] 💥 Error processing {zone['name']}: {e}")

        if packets:
            self._output_czml("atmospheric_friction", packets)
            print(f"[L23] ✔️ Map updated with {len(packets)} events.")
        else:
            print("[L23] 💤 No kinetic weather events found in this sweep.")

    async def l24_global_metabolism(self):
        """
        L24: Global Metabolism (Trade, Migration, Friction).
        Unified ecosystem module using UNHCR Population Statistics, World Bank, and CBP.
        """
        current_year = datetime.now().year
        
        # 1. TRADE: World Bank (Exports)
        URL_TRADE = "https://api.worldbank.org/v2/country/all/indicator/NE.EXP.GNFS.CD?format=json&per_page=60"
        
        # 2. BORDER: CBP (Real-time Wait Times)
        URL_BORDER = "https://bwt.cbp.gov/api/borderwaittimes"
        
        # 3. MIGRATION: UNHCR Population Statistics (Asylum Applications)
        URL_MIGRATION = (
            f"https://api.unhcr.org/population/v1/asylum-applications/?"
            f"yearFrom={current_year - 1}&yearTo={current_year}&limit=100&coo_all=true&coa_all=true"
        )

        print(f"\n[L24] 🛰️ SYNCING GLOBAL METABOLISM (YEAR: {current_year})...")

        async with httpx.AsyncClient(timeout=30.0, follow_redirects=True) as client:
            try:
                czml_package = [{"id": "document", "name": "GlobalMetabolism", "version": "1.0"}]
                
                # --- PHASE 1: TRADE ---
                r_t = await client.get(URL_TRADE)
                if r_t.status_code == 200:
                    trade_json = r_t.json()
                    if len(trade_json) > 1:
                        for entry in trade_json[1]:
                            if entry.get('value'):
                                # Fix: Ensuring self is used and passing 3 data arguments
                                packet = self._build_trade_packet(
                                    entry['countryiso3code'], 
                                    entry['country']['value'], 
                                    entry['value']
                                )
                                if packet:
                                    czml_package.append(packet)

                # --- PHASE 2: MIGRATION ---
                r_m = await client.get(URL_MIGRATION)
                if r_m.status_code == 200:
                    for flow in r_m.json().get('items', []):
                        # Fix: Ensure your _build_migration_packet also accepts 'self'
                        p = self._build_migration_packet(
                            flow['coo_name'], 
                            flow['coa_name'], 
                            flow['applied']
                        )
                        if p:
                            czml_package.append(p)

                # --- PHASE 3: BORDER ---
                r_b = await client.get(URL_BORDER)
                if r_b.status_code == 200:
                    for port in r_b.json()[:30]:
                        wait = port.get('passenger_wait_time', 0)
                        if wait > 0:
                            # Fix: Ensure your _build_friction_packet also accepts 'self'
                            p = self._build_friction_packet(
                                port['port_name'], 
                                [port['latitude'], port['longitude']], 
                                wait
                            )
                            if p:
                                czml_package.append(p)

                # --- UPLINK ---
                if len(czml_package) > 1:
                    self._output_czml("metabolism", czml_package)
                    print(f"  [📈] CZML PACKAGE BROADCAST: {len(czml_package)} entities.")

            except Exception as e:
                print(f"  [🛑] CZML GENERATION ERROR: {e}")

    async def l25_arms_shipments(self):
        """
        L25: GLOBAL ARMS NEXUS (Defense Procurement)
        Zero RSS Dependency. Relies entirely on DINEI_MOTHER OSINT.
        """
        from news_engine_vs import DINEI_MOTHER
        from curl_cffi.requests import AsyncSession
        
        engine = DINEI_MOTHER()
        
        arms_profile = {
            "name": "Defense Logistics Analyst",
            "goal": "Identify international arms sales, shipment of heavy weaponry, fighter jet deliveries, and missile contract signatures.",
            "tags": "F-35 delivery, Leopard 2 shipment, HIMARS transfer, defense contract, arms export",
            "vibe": "Industrial, Geopolitical, Tactical"
        }

        print("[L25] 📦 Deploying AI Analyst to hunt for global arms shipments...")
        try:
            async with AsyncSession(impersonate="chrome120") as client:
                # The engine hunts, geocodes, and returns localized intel
                results = await engine.fetch_and_process_dinei(client, arms_profile)

                if not results:
                    print("[L25] 📦 Analyst: No significant arms transfers detected in this sweep.")
                    return  # <--- CHANGED FROM: await asyncio.sleep(600); continue

                packets = []
                for deal in results:
                    loc = deal.get('location', {})
                    lat, lon = loc.get('lat'), loc.get('lon')
                    if not lat or not lon: continue

                    deal_id = f"ARMS_{str(deal.get('timestamp', '0'))[-6:]}"
                    
                    # Tactical Red Point for Kinetic Shipments
                    packets.append({
                        "id": deal_id,
                        "name": f"SHIPMENT: {deal.get('title')}",
                        "position": {"cartographicDegrees": [lon, lat, 0]},
                        "point": {
                            "pixelSize": 14,
                            "color": {"rgba": [255, 69, 0, 200]}, # Orange-Red
                            "outlineColor": {"rgba": [255, 255, 255, 255]},
                            "outlineWidth": 2
                        },
                        "description": f"INTEL: {deal.get('context')}\nSource: {deal.get('attribution', {}).get('source')}"
                    })

                if packets:
                    self._output_czml("arms_shipments", packets)

        except Exception as e:
            print(f"[L25] 💥 Arms Tracking System Failure: {e}")

    async def l27_sovereign_alert_status(self):
        """
        L32: SOVEREIGN ALERT STATUS
        Source: GDACS (UN/EC)
        Fix: Corrected XPath traversal for nested geo:Point tags.
        """
        GDACS_URL = "https://www.gdacs.org/xml/rss.xml"
        
        # Exact namespaces from your XML snippet
        NS = {
            'geo': 'http://www.w3.org/2003/01/geo/wgs84_pos#',
            'gdacs': 'http://www.gdacs.org',
            'georss': 'http://www.georss.org/georss'
        }

        print("[L32] 🛡️ Scanning GDACS for Sovereign-Level Emergencies...")
        packets = []
        try:
            async with AsyncSession() as s:
                r = await s.get(GDACS_URL)
                root = ET.fromstring(r.content)
                
                for item in root.findall('.//item'):
                    # 1. Get Alert Level (Direct child)
                    alert_el = item.find('gdacs:alertlevel', NS)
                    # 2. Get Coords (Nested inside geo:Point)
                    # We use 'geo:Point/geo:lat' to drill down
                    lat_el = item.find('geo:Point/geo:lat', NS)
                    lon_el = item.find('geo:Point/geo:long', NS)
                    
                    title_el = item.find('title')

                    # Safety Check: Skip items missing critical geo-data
                    if all(el is not None for el in [alert_el, lat_el, lon_el]):
                        alert_level = alert_el.text # e.g., "Green", "Orange", "Red"
                        
                        # Filter for Sovereign Impact (You can add "Green" for testing, 
                        # but "Orange/Red" are the real alerts)
                        if alert_level in ["Red", "Orange", "Green"]:
                            lat = float(lat_el.text)
                            lon = float(lon_el.text)
                            title = title_el.text if title_el is not None else "Unknown Event"
                            
                            # Visual logic based on GDACS severity
                            color = [255, 0, 0, 255] if alert_level == "Red" else [255, 165, 0, 255]
                            if alert_level == "Green": color = [0, 255, 0, 255]

                            packets.append({
                                "id": f"SOV_{hashlib.md5(title.encode()).hexdigest()[:8]}",
                                "name": f"GDACS: {title}",
                                "position": {"cartographicDegrees": [lon, lat, 3000]},
                                "point": {
                                    "pixelSize": 15 if alert_level != "Green" else 8,
                                    "color": {"rgba": color},
                                    "outlineColor": {"rgba": [255, 255, 255, 255]},
                                    "outlineWidth": 1
                                },
                                "description": (
                                    f"<b>Emergency Level:</b> {alert_level}<br/>"
                                    f"<b>Location:</b> {item.find('gdacs:country', NS).text}<br/>"
                                    f"<b>Event Type:</b> {item.find('gdacs:eventtype', NS).text}<br/>"
                                    f"<b>Intel:</b> {item.find('description').text}"
                                )
                            })
            
            if packets:
                self._output_czml("sovereign_alerts", packets)
                print(f"[L32] ✔️ {len(packets)} Events synced to Sovereign Layer.")
            else:
                print("[L32] 🔎 Scan complete. No critical alerts found.")

        except Exception as e:
            print(f"[!] L32 Parser Error: {e}")

    async def l28_urban_signal_density(self):
        """
        L33: URBAN SIGNAL DENSITY & ENVIRONMENTAL TRAFFIC FRICTION (OPENWEATHERMAP)
        Replaces: TomTom / Waze / HERE
        Requirements: 100% Free, NO Credit Card, NO BBox Area/Fragmentation locks.
        Logic: Samples target zone grids for real-time atmospheric degradation 
               (visibility metrics, precip load, convective storm alerts) 
               to mathematically derive active street network friction vectors.
        """
        # Secure your key instantly at: https://openweathermap.org/
        OWM_KEY = "5b2132ae80f6afa910033c350a8f9fa0"
        
        async with AsyncSession() as session:
            try:
                print("\n[L33] 📡 Synthesizing Urban Traffic Friction Matrix via OWM Telemetry...")
                packets = []
                
                for city, coords in self.ZONES.items():
                    bl_lat, bl_lon = map(float, coords["bl"].split(","))
                    tr_lat, tr_lon = map(float, coords["tr"].split(","))
                    
                    # Generate a high-fidelity 3-point sample grid per zone to populate density layers
                    center_lat = (bl_lat + tr_lat) / 2
                    center_lon = (bl_lon + tr_lon) / 2
                    
                    sample_nodes = [
                        {"name": f"{city.upper()} CORE METRO", "lat": center_lat, "lon": center_lon},
                        {"name": f"{city.upper()} NORTH-EAST TRANSIT", "lat": tr_lat, "lon": tr_lon},
                        {"name": f"{city.upper()} SOUTH-WEST ARTERIAL", "lat": bl_lat, "lon": bl_lon}
                    ]
                    
                    for node in sample_nodes:
                        url = "https://api.openweathermap.org/data/2.5/weather"
                        params = {
                            "lat": node["lat"],
                            "lon": node["lon"],
                            "appid": OWM_KEY,
                            "units": "metric"
                        }
                        
                        r = await session.get(url, params=params, timeout=15)
                        
                        if r.status_code == 200:
                            data = r.json()
                            
                            # 1. Extract raw traffic friction variables safely
                            visibility = data.get("visibility")
                            if visibility is None:
                                visibility = 10000
                                
                            wind_speed = (data.get("wind") or {}).get("speed")
                            if wind_speed is None:
                                wind_speed = 0.0
                                
                            rain_1h = (data.get("rain") or {}).get("1h")
                            if rain_1h is None:
                                rain_1h = 0.0
                                
                            snow_1h = (data.get("snow") or {}).get("1h")
                            if snow_1h is None:
                                snow_1h = 0.0
                            
                            weather_desc = "Clear Operations"
                            weather_list = data.get("weather", [])
                            if weather_list and isinstance(weather_list, list):
                                weather_desc = (weather_list[0] or {}).get("description", "Normal").upper()
                            
                            # 2. Compute algorithmic traffic delay coefficient (0 to 4 Scale)
                            friction_score = 0
                            delay_reason = "Fluid Traffic Flow"
                            
                            # Visibility penalties (Major cause of vehicle deceleration)
                            if visibility < 1000:       # Dense fog/hazard
                                friction_score += 3
                                delay_reason = "CRITICAL VISIBILITY BLACKOUT"
                            elif visibility < 4000:     # Moderate impairment
                                friction_score += 2
                                delay_reason = "MODERATE VISIBILITY REDUCTION"
                            elif visibility < 8000:     # Minor slowdowns
                                friction_score += 1
                                delay_reason = "CAUTION: LIGHT VISIBILITY HAZE"
                                
                            # Precipitation scaling (Hydroplaning risks & lane drop simulation)
                            if rain_1h > 5.0 or snow_1h > 2.0:   # Heavy downpour/blizzard
                                friction_score += 2
                                delay_reason = "FLASH FLOODING / SURFACE INUNDATION"
                            elif rain_1h > 1.0 or snow_1h > 0.5: # Moderate rain
                                friction_score = max(friction_score, 2)
                                delay_reason = "WET PAVEMENT BRAKING DELAYS"
                                
                            # Extreme wind drag threshold (Transit/High-profile vehicle blockages)
                            if wind_speed > 15.0:
                                friction_score = max(friction_score, 3)
                                delay_reason = "HIGH WIND TRANSIT OBSTRUCTION"

                            # Clamp friction index between baseline and extreme ceiling
                            friction_score = min(max(friction_score, 0), 4)
                            
                            # 3. Map values to your custom VisionSphere visual spec
                            if friction_score >= 3:
                                color = [255, 0, 0, 255]      # Critical Delay (Red)
                                pixel_size = 18
                                status_lbl = "GRIDLOCK RISK"
                            elif friction_score == 2:
                                color = [255, 140, 0, 255]    # Major Congestion (Orange)
                                pixel_size = 14
                                status_lbl = "HEAVY FRICTION"
                            elif friction_score == 1:
                                color = [255, 255, 0, 255]    # Minor Delay (Yellow)
                                pixel_size = 11
                                status_lbl = "SLOWING FLOW"
                            else:
                                color = [0, 255, 255, 200]    # Unimpeded (Cyan)
                                pixel_size = 8
                                status_lbl = "OPTIMAL SPEED"

                            packets.append({
                                "id": f"OWM_FRICTION_{node['lat']}_{node['lon']}",
                                "name": f"TRAFFIC: {node['name']}",
                                "position": {"cartographicDegrees": [node["lon"], node["lat"], 0]},
                                "point": {
                                    "pixelSize": pixel_size,
                                    "color": {"rgba": color},
                                    "outlineColor": {"rgba": [255, 255, 255, 255]},
                                    "outlineWidth": 1
                                },
                                "description": (
                                    f"<b>Sector Vector:</b> {node['name']}<br/>"
                                    f"<b>Mobility Index:</b> {status_lbl} ({friction_score}/4)<br/>"
                                    f"<b>Primary Friction Vector:</b> {delay_reason}<br/>"
                                    f"<b>Live Telemetry:</b> Visibility {visibility/1000:.1f}km | Rain: {rain_1h}mm/h | Wind: {wind_speed} m/s<br/>"
                                    f"<b>Reported Atmosphere:</b> {weather_desc}"
                                )
                            })
                        else:
                            # FIX 1: Provide immediate console visibility on API blocks/invalid keys
                            print(f" [⚠️] OWM API Connection Warning: HTTP {r.status_code} for {node['name']} - Verify your OWM_KEY.")
                        
                        # Sequential throttle safety to protect free tier limit (60 RPM)
                        await asyncio.sleep(1.2)
                        
                if packets:
                    self._output_czml("urban_friction_v5", packets)
                    print(f" [>] MATRIX UPDATE: Broadcasted {len(packets)} weather-traffic points to frontend layer.")
                else:
                    # FIX 2: Clear alert so you know why the loop went silent
                    print(" [⚠️] MATRIX SWEEP INCOMPLETE: No metric assets retrieved from OWM this cycle.")

            except Exception as e:
                print(f"[❌] L33 OpenWeather Core Processing Error: {e}")
            

    async def l29_crowd_gathering_matrix(self):
        """
        L34: LIVE CROWD GATHERING & HUMAN DENSITY VECTOR (PredictHQ)
        Logic: Live-queries high-impact public events (protests, concerts, festivals).
        Benefit: Yields exact predicted attendance numbers to visualize human massing.
        """
        # Get an API token from predicthq.com
        PREDICTHQ_TOKEN = os.getenv("PREDICTHQ_TOKEN")
        
        async with AsyncSession() as session:
            try:
                print("\n[L34] 👥 Scanning for High-Density Crowd Gatherings...")
                headers = {
                    "Authorization": f"Bearer {PREDICTHQ_TOKEN}",
                    "Accept": "application/json"
                }
                
                all_packets = []
                now_str = datetime.now(timezone.utc).strftime("%Y-%m-%d")

                for city, coords in self.ZONES.items():
                    bl_lat, bl_lon = map(float, coords["bl"].split(","))
                    tr_lat, tr_lon = map(float, coords["tr"].split(","))
                    center_lat = (bl_lat + tr_lat) / 2
                    center_lon = (bl_lon + tr_lon) / 2

                    # Search within a 30km radius of your tracked zone center points
                    params = {
                        "within": f"30km@{center_lat},{center_lon}",
                        "active.gte": now_str,
                        "active.lte": now_str,
                        "limit": 20,
                        "sort": "rank" # Prioritize by biggest attendance impact
                    }

                    url = "https://api.predicthq.com/v1/events/"
                    r = await session.get(url, headers=headers, params=params, timeout=15)
                    
                    if r.status_code == 200:
                        data = r.json()
                        events = data.get("results", [])
                        
                        for ev in events:
                            title = ev.get("title")
                            category = ev.get("category", "community")
                            
                            # FIX: Force fallback to 0 if the API explicitly returns null/None
                            attendance = ev.get("phq_attendance")
                            if attendance is None:
                                attendance = 0
                            
                            # PredictHQ geo object is GeoJSON: [longitude, latitude]
                            geo = ev.get("geo", {}).get("geometry", {})
                            if geo.get("type") == "Point":
                                lon = geo["coordinates"][0]
                                lat = geo["coordinates"][1]
                                
                                # Make visualization scale completely on the crowd size
                                # Larger crowd = Larger pulsing circle on VisionSphere
                                pulse_radius = max(500, int(attendance * 0.5)) 
                                
                                # Set transparency based on crowding strength
                                alpha = min(220, 100 + int(attendance * 0.01))

                                all_packets.append({
                                    "id": f"CROWD_{ev.get('id')}",
                                    "name": f"CROWD GATHERING: {title}",
                                    "position": {"cartographicDegrees": [lon, lat, 0]},
                                    "ellipse": {
                                        "semiMajorAxis": pulse_radius,
                                        "semiMinorAxis": pulse_radius,
                                        "material": {"solidColor": {"color": {"rgba": [0, 191, 255, alpha]}}}, # Cyan crowd zone
                                        "outline": True,
                                        "outlineColor": {"rgba": [255, 255, 255, 255]}
                                    },
                                    "description": (
                                        f"<b>Gathering Name:</b> {title}<br/>"
                                        f"<b>Classification:</b> {category.upper()}<br/>"
                                        f"<b>Predicted Population:</b> {attendance:,} people<br/>"
                                        f"<b>Tracking Status:</b> Dynamic Event Alert"
                                    )
                                })

                if all_packets:
                    self._output_czml("crowd_gatherings", all_packets)
                    print(f" [>] CROWD INTELLIGENCE: Synchronized {len(all_packets)} dense crowd assets.")

            except Exception as e:
                print(f"[❌] L34 Crowd Gathering Processing Error: {e}")

    # =================================================================
    # LAYER 28: CARRIER STRIKE & ESCORT RADIUS (TACTICAL GEOMETRY)
    # =================================================================
    def _flatten_coords(self, polygon_outer_ring: list) -> list:
        """
        Transforms multidimensional GeoJSON linear rings into flat 
        unidimensional cartographic arrays required for native CZML structures.
        """
        flat_list = []
        for coord in polygon_outer_ring:
            # Append Longitude, Latitude, and set Elevation/Altitude explicitly to 0 (sea-level)
            flat_list.extend([float(coord[0]), float(coord[1]), 0.0])
        return flat_list
        
    async def l30_carrier_strike_radius(self):
        """
        L28: CARRIER STRIKE GROUPS & TACTICAL RADIUS
        Zero AIS Stream Dependency. Relies entirely on DINEI_MOTHER OSINT news-intelligence extraction.
        Generates both the carrier position feature and its dynamic geodetic 500km strike polygon.
        """
        from news_engine_vs import DINEI_MOTHER
        from curl_cffi.requests import AsyncSession
        
        # Instantiate the Core Intelligence Processing Engine
        engine = DINEI_MOTHER()
        
        # Craft a highly specialized Profile for the AI Analyst
        carrier_profile = {
            "name": "Naval Strike Group & Carrier Deployment Analyst",
            "goal": (
                "Identify and extract locations of active aircraft carrier deployments, "
                "carrier strike group (CSG) transits, maritime strike group operations, "
                "amphibious assault ship positions, and strategic sovereign naval deployments worldwide."
            ),
            "tags": "aircraft carrier deployment, carrier strike group, CVN deployment, transit, naval exercise, strike group",
            "vibe": "Strategic, Sovereign-Level, Maritime-Command"
        }

        print("[L28] ⚓ Carrier Tracking Geometry Engine Online. Monitoring OSINT Intel...")

        print("[L28] 📡 Deploying AI Intelligence Analyst to map global Carrier positions...")
        try:
            async with AsyncSession(impersonate="chrome120") as client:
                # Execute the stealth search, parsing, extraction, and precise geocoding pipeline
                results = await engine.fetch_and_process_dinei(client, carrier_profile)

                if not results:
                    print("[L28] ⚓ Analyst Sweep Complete: No new carrier movements detected in this window.")
                    return  # <--- CHANGED FROM: await asyncio.sleep(600); continue

                packets = []
                for intelligence in results:
                    loc = intelligence.get('location', {})
                    lat, lon = loc.get('lat'), loc.get('lon')
                    
                    # Validate geocoding results
                    if not lat or not lon: 
                        continue

                    title = intelligence.get('title', 'Unknown Naval Asset')
                    context = intelligence.get('context', 'No additional tactical context provided.')
                    source_info = intelligence.get('attribution', {}).get('source', 'OSINT Source')
                    
                    # Dynamic ID generation using the timestamp signature
                    timestamp_str = str(intelligence.get('timestamp', '0'))[-6:]
                    carrier_id = f"CVN_{timestamp_str}"
                    zone_id = f"ZONE_{carrier_id}"
                    
                    # Standard modern military carrier strike footprint (Combat Air Wing Radius ~500.0 km)
                    radius_km = 500.0 
                    theme_color = "#FF4500" # Tactical Orange-Red for Strike Assertions

                    # --- GEODETIC CIRCLE MATHEMATICS (Post-GIS Free) ---
                    # Calculates a true geodetically compensated circle on a 3D globe 
                    flat_coords = []
                    for i in range(0, 361, 10):  # 10-degree increments for sharp mapping bounds
                        angle = math.radians(i)
                        # Compensate for longitude lines converging at the poles
                        p_lat = lat + (radius_km * math.sin(angle) / 111.0)
                        p_lon = lon + (radius_km * math.cos(angle) / (111.0 * math.cos(math.radians(lat))))
                        flat_coords.extend([p_lon, p_lat, 0.0])

                    # PACKET 1: The Carrier Capital Ship Point Element
                    packets.append({
                        "id": carrier_id,
                        "name": f"CAPITAL SHIP: {title}",
                        "position": {"cartographicDegrees": [lon, lat, 0.0]},
                        "point": {
                            "pixelSize": 15,
                            "color": {"rgba": [255, 69, 0, 255]}, # High visibility Solid Red-Orange
                            "outlineColor": {"rgba": [255, 255, 255, 255]},
                            "outlineWidth": 2.5
                        },
                        "label": {
                            "text": f"CSG: {title}",
                            "font": "11pt monospace",
                            "style": "FILL_AND_OUTLINE",
                            "horizontalOrigin": "LEFT",
                            "pixelOffset": {"cartesian2": [15, 0]},
                            "fillColor": {"rgba": [255, 255, 255, 255]},
                            "outlineColor": {"rgba": [0, 0, 0, 255]},
                            "outlineWidth": 2
                        },
                        "description": f"<b>TACTICAL REPORT:</b> {context}<br/><br/><b>INTEL SOURCE:</b> {source_info}"
                    })

                    # PACKET 2: The Associated 500km Strike Ring Element (Linked to the same carrier space)
                    packets.append({
                        "id": zone_id,
                        "name": f"STRIKE RADIUS: {title} ({radius_km}km)",
                        "polygon": {
                            "positions": {"cartographicDegrees": flat_coords},
                            "material": {
                                "solidColor": {
                                    "color": {"rgba": [255, 69, 0, 35]} # Translucent operational alpha overlay
                                }
                            },
                            "outline": True,
                            "outlineColor": {"rgba": [255, 69, 0, 200]},
                            "outlineWidth": 2.0,
                            "height": 0.0
                        },
                        "description": f"Tactical Exclusion/Strike Zone perimeter computed around {title} at {radius_km}km."
                    })

                    print(f"[⚓] L28 Mapped: {title} | Strike Footprint Generated [500km]")

                if packets:
                    # Direct, clean local pipeline output matching your file structure
                    self._output_czml("carrier_strike_radius", packets)

        except Exception as e:
            print(f"[L28] 💥 Carrier Tracking Engine Failure: {e}")

    async def l31_territorial_disputes(self):
        """
        L30: LIVE GLOBAL CONTESTED TERRITORIES & DISPUTED BOUNDARIES LAYER
        Completely dynamic local CZML generation layer. Zero database uplinks.
        Follows the exact 4-quadrant segmentation and local file compilation architecture of L29.
        """
        import asyncio
        import httpx
        from datetime import datetime, timezone

        overpass_url = "https://overpass-api.de/api/interpreter"
        
        # 4 Quadrants to safely segment global coordinate space and bypass timeouts
        quadrants = [
            {"name": "NORTH_WEST", "bbox": "0,-180,90,0"},
            {"name": "NORTH_EAST", "bbox": "0,0,90,180"},
            {"name": "SOUTH_WEST", "bbox": "-90,-180,0,0"},
            {"name": "SOUTH_EAST", "bbox": "-90,0,0,180"}
        ]

        # Authentic, high-yield contested boundary line tags from OSM
        tag_query = """
        (
          way["boundary"="disputed"];
          way["border_type"="territorial_dispute"];
          way["disputed"="yes"];
        )
        """

        headers = {
            "User-Agent": "VisionSphere_GeopoliticalIntel/3.0 (ops@visionsphere.internal)",
            "Accept": "application/json"
        }

        print("\n" + "="*50)
        print("[L30] 🌍 RUNNING LIVE GLOBAL TERRITORIAL DISPUTE CZML GENERATION...")
        print("="*50)
        
        all_dispute_packets = []
        
        # Insert native CZML initialization document envelope
        all_dispute_packets.append({
            "id": "document",
            "name": "Territorial Disputes & Unresolved Borders Layer",
            "version": "1.0"
        })
        
        # Ephemeral client layer ensures clean connection pool frames
        async with httpx.AsyncClient(timeout=120.0) as client:
            for q in quadrants:
                print(f"  [>] Querying live assets in {q['name']} Corridor...")
                query = f"[out:json][timeout:90][bbox:{q['bbox']}];{tag_query};out geom;"
                
                try:
                    resp = await client.post(overpass_url, data={'data': query}, headers=headers)

                    if resp.status_code == 200:
                        data = resp.json()
                        elements = data.get('elements', [])
                        
                        mapped_count = 0
                        for el in elements:
                            element_id = str(el.get('id'))
                            geometry_pts = el.get('geometry', [])
                            
                            if not geometry_pts or len(geometry_pts) < 2:
                                continue
                                
                            # Unpack node geometry string into flat array of CZML [lon, lat, alt] coordinates
                            czml_positions = []
                            for pt in geometry_pts:
                                czml_positions.extend([pt['lon'], pt['lat'], 0])
                            
                            # Completely dynamic property harvesting from live elements
                            tags = el.get('tags', {})
                            disputed_name = tags.get('name') or tags.get('name:en')
                            claimed_by = tags.get('claimed_by') or tags.get('disputed_by') or "Multiple Sovereign Claims"
                            left_country = tags.get('left:country') or "Unverified"
                            right_country = tags.get('right:country') or "Unverified"
                            border_type = tags.get('border_type', 'Disputed Boundary/Line of Control')
                            description = tags.get('description') or "Active non-demarcated or unilaterally asserted frontier line."
                            
                            if not disputed_name:
                                if tags.get('left:country') and tags.get('right:country'):
                                    disputed_name = f"Contested Frontier ({left_country} / {right_country})"
                                else:
                                    disputed_name = f"Unresolved Boundary Segment {element_id}"
                            
                            # Format visual text package layout for the mapping UI client
                            html_description = f"""
                            <div style="font-family: monospace; color: #FFF; padding: 6px; border-left: 3px solid #FF3333;">
                                <h3 style="color: #FF3333; margin: 0 0 8px 0;">⚠️ {disputed_name}</h3>
                                <b>Classification:</b> {border_type}<br/>
                                <b>Sovereignty Claims:</b> {claimed_by}<br/>
                                <b>Adjacent Jurisdictions:</b> {left_country} vs {right_country}<br/>
                                <b>Summary:</b> {description}<br/>
                                <span style="font-size: 9px; color: #666;">OSM Reference Index: {element_id}</span>
                            </div>
                            """

                            # Construct standard line packet targeting the geospatial local engine layout
                            packet = {
                                "id": f"DISPUTE_{element_id}",
                                "name": disputed_name,
                                "description": html_description,
                                "polyline": {
                                    "positions": {
                                        "cartographicDegrees": czml_positions
                                    },
                                    "material": {
                                        "solidColor": {
                                            "color": {
                                                "rgba": [255, 51, 51, 230] # High-Visibility Tactical Red
                                            }
                                        }
                                    },
                                    "width": 4,
                                    "clampToGround": True
                                }
                            }
                            
                            all_dispute_packets.append(packet)
                            mapped_count += 1
                            
                        print(f"  [+] ✔️ {q['name']}: Dynamically pulled and formatted {mapped_count} conflict vectors.")

                    elif resp.status_code == 429:
                        print(f"  [!] ⚠️ Overpass server busy during {q['name']} scan. Cooling down...")
                        await asyncio.sleep(30)
                    else:
                        print(f"  [!] ❌ HTTP Error {resp.status_code} on {q['name']}.")

                except Exception as e:
                    print(f"  [!] 💥 Pipeline error processing grid {q['name']}: {e}")

                # 15-second slot-waiter window matches L29 to prevent engine IP blocks
                await asyncio.sleep(15)

        # Ship compiled geometric payload directly to local CZML tracker file
        if len(all_dispute_packets) > 1:
            self._output_czml("territorial_disputes", all_dispute_packets)
            print(f"\n[L30] 🏁 STREAM UPDATE COMPLETE: {len(all_dispute_packets) - 1} live border lines compiled to CZML pipeline.")
        else:
            print("\n[!] L30: Global sync sequence returned empty payload. Verifying API queries.")

    def _append_sonar_components(self, packets, node_id, name, lat, lon, node_type, category, operator, coverage_radius_km):
        """Compiles real-time geometric and token data straight into client packet buffers"""
        import math
        theme_color = [0, 191, 255, 255] # Electric Deep Cyan
        
        # --- DYNAMIC GEODETIC COORDINATE COMPENSATOR ---
        flat_coords = []
        for i in range(0, 361, 15): 
            angle = math.radians(i)
            p_lat = lat + (coverage_radius_km * math.sin(angle) / 111.0)
            p_lon = lon + (coverage_radius_km * math.cos(angle) / (111.0 * math.cos(math.radians(lat))))
            flat_coords.extend([p_lon, p_lat, 0.0])

        # Live Physical Terminal Node
        packets.append({
            "id": f"MAR_NODE_{node_id}",
            "name": f"STATION: {name}",
            "position": {"cartographicDegrees": [lon, lat, 0.0]},
            "point": {
                "pixelSize": 10,
                "color": {"rgba": theme_color},
                "outlineColor": {"rgba": [255, 255, 255, 255]},
                "outlineWidth": 1.5
            },
            "label": {
                "text": name if len(name) < 25 else "Live Marine Station",
                "font": "10pt Monaco, monospace",
                "fillColor": {"rgba": [255, 255, 255, 255]},
                "outlineColor": {"rgba": [0, 0, 0, 255]},
                "outlineWidth": 2,
                "style": "FILL_AND_OUTLINE",
                "verticalOrigin": "BOTTOM",
                "pixelOffset": {"cartesian2": [0, -12]},
                "disableDepthTestDistance": 1000000
            },
            "description": (
                f"<b>INFRASTRUCTURE TYPE:</b> {node_type}<br/>"
                f"<b>FUNCTION/TARGET:</b> {category.upper()}<br/>"
                f"<b>OPERATING AGENCY:</b> {operator}<br/><br/>"
                f"<b>COORDINATES:</b> {lat}, {lon}<br/>"
                f"<b>DATA FEED:</b> Live OpenStreetMap Infrastructure Stream"
            )
        })

        # Live Intercept/Coverage Boundary Polygon
        packets.append({
            "id": f"MAR_RANGE_{node_id}",
            "name": f"COVERAGE DIAMETER: {name}",
            "polygon": {
                "positions": {"cartographicDegrees": flat_coords},
                "material": {
                    "solidColor": {
                        "color": {"rgba": [0, 191, 255, 15]} # Highly translucent operational footprint
                    }
                },
                "outline": True,
                "outlineColor": {"rgba": [0, 191, 255, 100]},
                "outlineWidth": 1.0,
                "height": 0.0
            },
            "description": f"Calculated range signature coverage radius ({coverage_radius_km}km) around dynamic station node {node_id}."
        })

    def _generate_circle_polygon(self, lon: float, lat: float, radius_km: float, steps: int = 36) -> list:
        """
        Calculates a geodetically accurate surface circle footprint array.
        Returns a GeoJSON-compliant structured list format: [[ [lon1, lat1], [lon2, lat2], ... ]]
        """
        coordinates = []
        # Earth's approximate radius in kilometers
        earth_radius = 6371.0  

        for i in range(steps + 1):
            # Equal angular step distribution around 360 degrees
            bearing = math.radians((i * 360.0) / steps)
            
            lat_rad = math.radians(lat)
            lon_rad = math.radians(lon)
            
            # Angular distance parameter calculation
            angular_dist = radius_km / earth_radius
            
            # Spherical law of cosines transformations
            dest_lat_rad = math.asin(
                math.sin(lat_rad) * math.cos(angular_dist) +
                math.cos(lat_rad) * math.sin(angular_dist) * math.cos(bearing)
            )
            
            dest_lon_rad = lon_rad + math.atan2(
                math.sin(bearing) * math.sin(angular_dist) * math.cos(lat_rad),
                math.cos(angular_dist) - math.sin(lat_rad) * math.sin(dest_lat_rad)
            )
            
            # Normalize longitude vector bounds between -180 and +180
            dest_lon_deg = math.degrees(dest_lon_rad)
            dest_lon_deg = (dest_lon_deg + 540) % 360 - 180
            
            coordinates.append([dest_lon_deg, math.degrees(dest_lat_rad)])
            
        return [coordinates]

    async def l32_territorial_disputes(self):
        """
        L30: LIVE GLOBAL CONTESTED TERRITORIES & DISPUTED BOUNDARIES LAYER
        Fully customized parser optimized for Natural Earth 50m disputed areas schema.
        Extracts naming from 'BRK_NAME', classification via 'FEATURECLA', and context via 'NOTE'.
        Bypasses public Overpass limits to permanently eliminate Cloudflare 522 timeouts.
        """
        import asyncio
        import httpx
        from datetime import datetime, timezone

        # Target raw data stream URL matching the exact file layout verified by operator
        geojson_url = "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_50m_admin_0_boundary_lines_disputed_areas.geojson"
        
        headers = {
            "User-Agent": "VisionSphere_GeopoliticalIntel/3.0 (ops@visionsphere.internal)",
            "Accept": "application/json"
        }

        print("\n" + "="*60)
        print("[L30] 🌍 INITIATING LIVE DISPUTED AREA DATA RECONNAISSANCE...")
        print("="*60)
        
        all_dispute_packets = []
        
        # Append mandatory CZML document root configuration
        all_dispute_packets.append({
            "id": "document",
            "name": "Natural Earth Disputed Areas & Contested Sovereignty Lines",
            "version": "1.0"
        })
        
        try:
            print(f"  [>] Pulling verified GeoJSON asset vector matrix...")
            async with httpx.AsyncClient(timeout=60.0) as client:
                resp = await client.get(geojson_url, headers=headers)
                
                if resp.status_code == 200:
                    geojson_data = resp.json()
                    features = geojson_data.get("features", [])
                    print(f"  [+] Download complete. Parsing {len(features)} active operational records...")
                    
                    mapped_count = 0
                    for index, feat in enumerate(features):
                        props = feat.get("properties", {})
                        geom = feat.get("geometry", {})
                        geom_type = geom.get("type")
                        
                        # Extract explicit telemetry keys matching verified schema rules
                        brk_name = props.get("BRK_NAME")
                        feature_class = props.get("FEATURECLA") or "Claim boundary"
                        note = props.get("NOTE") or "Geopolitical frontier line under active dispute."
                        ne_id = props.get("NE_ID") or f"GEN_{index}"
                        
                        # Safeguard fallback if BRK_NAME is completely missing
                        if not brk_name:
                            brk_name = f"Unclassified Contested Segment (ID: {ne_id})"
                        
                        # Standardize incoming coordinate arrays for line components
                        coordinate_tracks = []
                        if geom_type == "LineString":
                            coordinate_tracks.append(geom.get("coordinates", []))
                        elif geom_type == "MultiLineString":
                            coordinate_tracks.extend(geom.get("coordinates", []))
                        else:
                            # Skips polygon outlines or empty spatial nodes safely
                            continue
                        
                        # Unpack spatial tracks into CZML flat polyline data frames
                        for track_idx, track in enumerate(coordinate_tracks):
                            if not track or len(track) < 2:
                                continue
                                
                            czml_positions = []
                            for pt in track:
                                # Coordinates map to: [Longitude, Latitude, Altitude (0 Ground)]
                                czml_positions.extend([pt[0], pt[1], 0])
                                
                            # Build custom information display block for console UI overlay
                            html_description = f"""
                            <div style="font-family: monospace; color: #FFF; padding: 6px; border-left: 3px solid #FF3333; background: rgba(20,20,20,0.5);">
                                <h3 style="color: #FF3333; margin: 0 0 8px 0;">⚠️ {brk_name}</h3>
                                <b>Sovereignty Class:</b> {feature_class}<br/>
                                <b>Operational Note:</b> {note}<br/>
                                <hr style="border: 0; border-top: 1px solid #444; margin: 8px 0;"/>
                                <span style="font-size: 10px; color: #AAA;">Vector ID: NE_{ne_id} | Scale Rank: {props.get('SCALERANK')}</span>
                            </div>
                            """
                            
                            # Generate safe structural tracking packet
                            packet = {
                                "id": f"NE_DISPUTE_{ne_id}_{track_idx}",
                                "name": f"{brk_name} ({feature_class})",
                                "description": html_description,
                                "polyline": {
                                    "positions": {
                                        "cartographicDegrees": czml_positions
                                    },
                                    "material": {
                                        "solidColor": {
                                            "color": {
                                                # Blazing tactical red line wrapper with alpha opacity
                                                "rgba": [255, 51, 51, 240] 
                                            }
                                        }
                                    },
                                    "width": 4,
                                    "clampToGround": True
                                }
                            }
                            
                            all_dispute_packets.append(packet)
                            mapped_count += 1
                            
                    print(f"  [+] ✔️ Geo-parsing successful. Processed {mapped_count} tracking track vectors.")
                else:
                    print(f"  [!] ❌ Failed to fetch asset from mirror frame. Code response: {resp.status_code}")
                    
        except Exception as e:
            print(f"  [!] 💥 Critical extraction failure inside L30 loop: {e}")

        # Send completed tracking bundle frame to your system engine output point
        if len(all_dispute_packets) > 1:
            self._output_czml("territorial_disputes", all_dispute_packets)
            print(f"[L30] 🏁 EXPORT COMPLETE: Layer tracking file populated with {len(all_dispute_packets) - 1} records.")
        else:
            print("[!] L30: Engine trace yielded a blank spatial matrix array.")

    async def l33_maritime_disputes(self):
        """
        L31: LIVE GLOBAL MARITIME DISPUTES & GEOPOLITICAL INDICATOR LAYER
        Fetches, merges, and parses international and regional maritime indicator limits.
        Utilizes Natural Earth 10m high-resolution maritime vector boundaries.
        Bypasses traditional rate-limited API pipelines by targeting raw geojson asset nodes directly.
        """
        import asyncio
        import httpx
        from datetime import datetime, timezone

        # Target raw data stream URLs provided by the operator
        endpoints = {
            "global": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_boundary_lines_maritime_indicator.geojson",
            "chn_view": "https://raw.githubusercontent.com/nvkelso/natural-earth-vector/master/geojson/ne_10m_admin_0_boundary_lines_maritime_indicator_chn.geojson"
        }
        
        headers = {
            "User-Agent": "VisionSphere_GeopoliticalIntel/3.0 (ops@visionsphere.internal)",
            "Accept": "application/json"
        }

        print("\n" + "="*60)
        print("[L31] 🌊 INITIATING LIVE MARITIME VECTOR RECONNAISSANCE...")
        print("="*60)
        
        all_maritime_packets = []
        
        # Append mandatory CZML document root configuration
        all_maritime_packets.append({
            "id": "document",
            "name": "Natural Earth 10m Maritime Boundary Indicators & Disputed Claims",
            "version": "1.0"
        })
        
        async with httpx.AsyncClient(timeout=60.0) as client:
            for view_type, geojson_url in endpoints.items():
                try:
                    print(f"  [>] Pulling verified {view_type.upper()} maritime asset vector matrix...")
                    resp = await client.get(geojson_url, headers=headers)
                    
                    if resp.status_code == 200:
                        geojson_data = resp.json()
                        features = geojson_data.get("features", [])
                        print(f"  [+] Download complete. Parsing {len(features)} {view_type} maritime records...")
                        
                        mapped_count = 0
                        for index, feat in enumerate(features):
                            props = feat.get("properties", {})
                            geom = feat.get("geometry", {})
                            geom_type = geom.get("type")
                            
                            # Extract metadata fields specific to the 10m indicator schemas
                            feature_class = props.get("FEATURECLA") or "Maritime Indicator Line"
                            name = props.get("NAME") or props.get("BRK_NAME")
                            note = props.get("NOTE") or "International or regional maritime boundary indicator vector."
                            ne_id = props.get("NE_ID") or f"MAR_{view_type}_{index}"
                            scale_rank = props.get("SCALERANK", 0)
                            
                            # Set context title fallback based on available attributes
                            if not name:
                                name = f"{feature_class} (ID: {ne_id})"
                            
                            # Standardize coordinate arrays for line components
                            coordinate_tracks = []
                            if geom_type == "LineString":
                                coordinate_tracks.append(geom.get("coordinates", []))
                            elif geom_type == "MultiLineString":
                                coordinate_tracks.extend(geom.get("coordinates", []))
                            else:
                                # Safe skip for point anomalies or empty spatial nodes
                                continue
                            
                            # Process layout variant to color-code the operational views
                            # Blazing tactical cyan for global standards, deep magenta for regional claims
                            stroke_color = [57, 255, 20, 240] if view_type == "global" else [255, 0, 127, 240]
                            
                            # Unpack spatial tracks into CZML flat polyline data frames
                            for track_idx, track in enumerate(coordinate_tracks):
                                if not track or len(track) < 2:
                                    continue
                                    
                                czml_positions = []
                                for pt in track:
                                    # Coordinates map to: [Longitude, Latitude, Altitude (0 Ground)]
                                    czml_positions.extend([pt[0], pt[1], 0])
                                    
                                # Build custom intelligence briefing block for UI overlay displays
                                html_description = f"""
                                <div style="font-family: monospace; color: #FFF; padding: 6px; border-left: 3px solid #39FF14; background: rgba(20,20,20,0.6);">
                                    <h3 style="color: #39FF14; margin: 0 0 8px 0;">⚓ {name}</h3>
                                    <b>Boundary Type:</b> {feature_class}<br/>
                                    <b>Dataset Source Context:</b> {view_type.upper()} Perspectives Data<br/>
                                    <b>Intelligence Note:</b> {note}<br/>
                                    <hr style="border: 0; border-top: 1px solid #444; margin: 8px 0;"/>
                                    <span style="font-size: 10px; color: #AAA;">Vector ID: NE_{ne_id} | Track Index: {track_idx} | Scale: {scale_rank}</span>
                                </div>
                                """
                                
                                # Generate safe tracking packet
                                packet = {
                                    "id": f"NE_MARITIME_{view_type}_{ne_id}_{track_idx}",
                                    "name": f"Maritime Ind: {name}",
                                    "description": html_description,
                                    "polyline": {
                                        "positions": {
                                            "cartographicDegrees": czml_positions
                                        },
                                        "material": {
                                            "solidColor": {
                                                "color": {
                                                    "rgba": stroke_color
                                                }
                                            }
                                        },
                                        "width": 3 if view_type == "global" else 4,
                                        "clampToGround": True
                                    }
                                }
                                
                                all_maritime_packets.append(packet)
                                mapped_count += 1
                                
                        print(f"  [+] ✔️ Map parsing successful. Extracted {mapped_count} tracking sub-vectors from {view_type}.")
                    else:
                        print(f"  [!] ❌ Failed to fetch asset mirror for {view_type}. Server responded: {resp.status_code}")
                        
                except Exception as e:
                    print(f"  [!] 💥 Critical extraction failure parsing {view_type} data framework: {e}")

        # Send complete tracking bundle frame to your output interface method
        if len(all_maritime_packets) > 1:
            self._output_czml("maritime_disputes", all_maritime_packets)
            print(f"[L31] 🏁 PACKET GENERATION REFRESH COMPLETE: Vector output synchronized with {len(all_maritime_packets) - 1} vectors.")
        else:
            print("[!] L31: Pipeline stream yielded a blank spatial matrix array.")

    async def l34_global_migration_flows(self):
        """
        L34: GLOBAL ASYLUM & MIGRATION FLOWS
        Dynamically fetches current year data and generates 3D geodesic arcs.
        """
        print("\n" + "="*60)
        print("[L34] 🚶 INITIATING GLOBAL MIGRATION FLOW SYNC (UNHCR)...")
        print("="*60)
        
        # Dynamically set the year
        current_year = datetime.now(timezone.utc).year
        unhcr_url = (
            f"https://api.unhcr.org/population/v1/asylum-applications/"
            f"?yearFrom={current_year - 1}&yearTo={current_year}&coo_all=true&coa_all=true&limit=100"
        )
        
        all_migration_packets = [{"id": "document", "name": "Global Migration Flows", "version": "1.0"}]
        
        try:
            async with httpx.AsyncClient(timeout=45.0) as client:
                resp = await client.get(unhcr_url)
                if resp.status_code == 200:
                    raw_items = resp.json().get("items", [])
                    
                    # 1. AGGREGATION STEP
                    flow_map = defaultdict(lambda: {"volume": 0, "o_name": "", "d_name": ""})
                    for item in raw_items:
                        o_iso = item.get("coo_iso")
                        d_iso = item.get("coa_iso")
                        
                        if o_iso in ["-", None] or d_iso in ["-", None]: continue
                        
                        key = (o_iso, d_iso)
                        flow_map[key]["volume"] += int(item.get("applied", 0))
                        flow_map[key]["o_name"] = item.get("coo_name")
                        flow_map[key]["d_name"] = item.get("coa_name")

                    # 2. GENERATION STEP
                    mapped_count = 0
                    for (o_iso, d_iso), data in flow_map.items():
                        if data["volume"] < 500: continue
                        
                        o_coords = GLOBAL_MAP.get(o_iso)
                        d_coords = GLOBAL_MAP.get(d_iso)
                        if not o_coords or not d_coords: continue
                        
                        o_lat, o_lon = o_coords
                        d_lat, d_lon = d_coords
                        
                        dist = math.sqrt((d_lon - o_lon)**2 + (d_lat - o_lat)**2)
                        arc_altitude = min(dist * 12000, 1000000)
                        line_width = min(max(2, data["volume"] / 2000), 12)
                        
                        packet = {
                            "id": f"MIG_{o_iso}_{d_iso}_{current_year}",
                            "polyline": {
                                "positions": {"cartographicDegrees": [o_lon, o_lat, 0, (o_lon+d_lon)/2, (o_lat+d_lat)/2, arc_altitude, d_lon, d_lat, 0]},
                                "material": {"polylineArrow": {"color": {"rgba": [255, 191, 0, 200]}}},
                                "width": line_width,
                                "arcType": "GEODESIC"
                            }
                        }
                        all_migration_packets.append(packet)
                        mapped_count += 1
                        
                    print(f" [+] Success: Mapped {mapped_count} unique migration corridors for {current_year}.")
                
        except Exception as e:
            print(f" [!] 💥 Extraction failure: {e}")

        if len(all_migration_packets) > 1:
            self._output_czml("migration_flows", all_migration_packets)

    # =================================================================
    # LAYER 35: STEALTH GHOST-PATH PREDICTION (DEAD RECKONING)
    # =================================================================

    async def l35_forest_habitable_land(self):
        """
        L35: GLOBAL FOREST COVER & HABITABLE LAND INDEX
        Dynamically extracts and aggregates sovereign environmental metrics 
        (Forest area % and Urban density %) across the entire planet via open 
        international development data gateways.
        """
        print("\n" + "="*60)
        print("[L35] 🌍 INITIATING GLOBAL TERRAIN & LAND COVER PROFILE SYNC...")
        print("="*60)
        
        # Initialize CZML payload with document tracking header
        packets = [{"id": "document", "name": "Global Land & Forest Cover Layer", "version": "1.0"}]
        
        forest_data_map = {}
        urban_data_map = {}
        country_names = {}
        
        headers = {"Accept": "application/json", "User-Agent": "VisionSphere_Core/3.0"}
        
        try:
            async with httpx.AsyncClient(timeout=30.0) as client:
                # 1. Fetch Global Forest Cover (% of total land area)
                # Querying a multi-year historical block to automatically catch the latest reporting cycle
                forest_url = "https://api.worldbank.org/v2/country/all/indicator/AG.LND.FRST.ZS?format=json&per_page=1500&date=2020:2025"
                f_resp = await client.get(forest_url, headers=headers)
                if f_resp.status_code == 200:
                    f_json = f_resp.json()
                    if len(f_json) > 1 and isinstance(f_json[1], list):
                        for record in f_json[1]:
                            iso = record.get("countryiso3code")
                            val = record.get("value")
                            # The API returns entries ordered chronologically descending; 
                            # caching the first non-null value captures the freshest data point.
                            if iso and val is not None and iso not in forest_data_map:
                                forest_data_map[iso] = round(val, 2)
                                country_names[iso] = record.get("country", {}).get("value", iso)
                
                # 2. Fetch Global Habitable Landscape Profile (% of total population residing in urban clusters)
                urban_url = "https://api.worldbank.org/v2/country/all/indicator/SP.URB.TOTL.IN.ZS?format=json&per_page=1500&date=2020:2025"
                u_resp = await client.get(urban_url, headers=headers)
                if u_resp.status_code == 200:
                    u_json = u_resp.json()
                    if len(u_json) > 1 and isinstance(u_json[1], list):
                        for record in u_json[1]:
                            iso = record.get("countryiso3code")
                            val = record.get("value")
                            if iso and val is not None and iso not in urban_data_map:
                                urban_data_map[iso] = round(val, 2)

            # 3. Synchronize Compiled Global Vectors to Coordinate Grid Matrix
            mapped_count = 0
            for iso, coords in GLOBAL_MAP.items():
                forest_val = forest_data_map.get(iso, 0.0)
                urban_val = urban_data_map.get(iso, 0.0)
                name = country_names.get(iso, iso)
                
                # Skip rendering if a localized territory contains zero baseline values 
                if forest_val == 0.0 and urban_val == 0.0:
                    continue
                    
                lat, lon = coords
                
                # Translate data metrics into proportional geometric properties for the 3D map engine
                cylinder_height = max(20000.0, forest_val * 8000.0)  # Vertical scale maps to Forest volume
                cylinder_radius = max(15000.0, urban_val * 4000.0)  # Horizontal coverage maps to Habitable footprint
                
                # Categorize terrain dynamics dynamically
                if forest_val > 40.0 and urban_val < 30.0:
                    profile = "High-Density Canopy (Optimal Concealment Zone)"
                    rgba_color = [34, 139, 34, 180]  # Forest Green
                elif urban_val > 60.0:
                    profile = "High-Density Habitable Infrastructure (Sighting Risk)"
                    rgba_color = [220, 20, 60, 180]   # Structural Crimson
                else:
                    profile = "Balanced Rural / Variable Low-Density Cover"
                    rgba_color = [218, 165, 32, 180]  # Tactical Ochre
                    
                packet = {
                    "id": f"ENV_MATRIX_{iso}",
                    "name": f"TERRAIN: {name}",
                    "position": {"cartographicDegrees": [lon, lat, cylinder_height / 2]},
                    "cylinder": {
                        "length": cylinder_height,
                        "topRadius": cylinder_radius,
                        "bottomRadius": cylinder_radius,
                        "material": {
                            "solidColor": {
                                "color": {
                                    "rgba": rgba_color
                                }
                            }
                        },
                        "outlineColor": {"rgba": [255, 255, 255, 120]},
                        "outlineWidth": 1
                    },
                    "description": (
                        f"📁 <b>{name} Land Cover Profile</b><br/>"
                        f"━━━━━━━━━━━━━━━━━━━━━<br/>"
                        f"🌲 Forest Cover Ratio: {forest_val}%<br/>"
                        f"🏙️ Habitable Urban Footprint: {urban_val}%<br/>"
                        f"⚔️ Tactical Designation: {profile}"
                    )
                }
                packets.append(packet)
                mapped_count += 1
                
            print(f" [+] Success: Compiled and structured global terrain matrices for {mapped_count} sovereign sectors.")
            
        except Exception as e:
            print(f" [!] Error compiling global environmental matrices: {e}")
            
        # If data was processed successfully, push to the engine file pipeline
        if len(packets) > 1:
            self._output_czml("forest_habitable_land", packets)
                

    # =================================================================
    # LAYER 36: GLOBAL ATMOSPHERIC DETONATION MATRIX (KINETIC SHOCKWAVES)
    # =================================================================
    async def l36_atmospheric_fireball_impacts(self):
        """
        L36: GLOBAL ATMOSPHERIC SHOCKWAVES & KINETIC FIREBALLS
        Queries NASA JPL open defense sensor feeds to map high-altitude atmospheric 
        detonations, plotting energy yields as 3D floating plasma horizons.
        """
        print("\n" + "="*60)
        print("[L36] ☄️  SYNCHRONIZING ATMOSPHERIC DETONATION & SHOCKWAVE RECORDS...")
        print("="*60)
        
        # Initialize clean CZML layout array
        packets = [{
            "id": "document", 
            "name": "Global Atmospheric Detonation Matrix", 
            "version": "1.0"
        }]
        
        # Direct access open NASA telemetry gate (returns last 100 major atmospheric events)
        nasa_url = "https://ssd-api.jpl.nasa.gov/fireball.api?limit=10000"
        headers = {"Accept": "application/json", "User-Agent": "VisionSphere_IntelCore/3.0"}
        
        try:
            async with httpx.AsyncClient(timeout=20.0) as client:
                resp = await client.get(nasa_url, headers=headers)
                
                if resp.status_code == 200:
                    payload = resp.json()
                    fields = payload.get("fields", [])
                    data_rows = payload.get("data", [])
                    
                    # Dynamically discover columns to prevent parsing breaks if upstream schema changes
                    idx_date = fields.index("date") if "date" in fields else 0
                    idx_energy = fields.index("energy") if "energy" in fields else 1
                    idx_lat = fields.index("lat") if "lat" in fields else 3
                    idx_lat_dir = fields.index("lat-dir") if "lat-dir" in fields else 4
                    idx_lon = fields.index("lon") if "lon" in fields else 5
                    idx_lon_dir = fields.index("lon-dir") if "lon-dir" in fields else 6
                    idx_alt = fields.index("alt") if "alt" in fields else 7
                    idx_vel = fields.index("vel") if "vel" in fields else 8
                    
                    mapped_count = 0
                    
                    for row in data_rows:
                        try:
                            raw_lat = row[idx_lat]
                            raw_lon = row[idx_lon]
                            raw_alt = row[idx_alt]
                            
                            # Skip entries missing vital coordinate vectors
                            if not raw_lat or not raw_lon:
                                continue
                                
                            # Convert Lat/Lon components based on hemispheric direction strings
                            lat = float(raw_lat)
                            if row[idx_lat_dir] == "S":
                                lat = -lat
                                
                            lon = float(raw_lon)
                            if row[idx_lon_dir] == "W":
                                lon = -lon
                                
                            # Altitude is reported in kilometers; default to 35km if sensor sweep missed exact burst height
                            alt_m = float(raw_alt) * 1000.0 if raw_alt else 35000.0
                            
                            date_str = row[idx_date]
                            velocity = row[idx_vel] if row[idx_vel] else "UNDETERMINED"
                            
                            # Energy yield expressed in terms of Kilotons (kt) of TNT detonated
                            energy_kt = float(row[idx_energy]) if row[idx_energy] else 0.08
                            
                            # Compute adaptive geometry size: Higher yield = wider shockwave bubble on map
                            sphere_radius = max(40000.0, energy_kt * 180000.0)
                            
                            # Format an isolated payload packet configuration
                            event_id = f"KINETIC_BURST_{date_str.replace(' ', '_').replace(':', '-')}"
                            
                            packet = {
                                "id": event_id,
                                "name": f"DETONATION: {energy_kt}kt TNT Yield",
                                # Completely suspended in space matching the atmospheric combustion altitude
                                "position": {"cartographicDegrees": [lon, lat, alt_m]},
                                "sphere": {
                                    "radii": {"cartographicDegrees": [sphere_radius, sphere_radius, sphere_radius]},
                                    "material": {
                                        "solidColor": {
                                            "color": {
                                                "rgba": [255, 99, 71, 140] # Vibrant Thermal Orange with translucency
                                            }
                                        }
                                    }
                                },
                                # Draw a tracking vector line down from the explosion directly to the ground target zone
                                "polyline": {
                                    "positions": {
                                        "cartographicDegrees": [
                                            lon, lat, alt_m,   # Start at high altitude point
                                            lon, lat, 0.0      # Terminate perfectly at surface level
                                        ]
                                    },
                                    "material": {
                                        "solidColor": {
                                            "color": {"rgba": [255, 255, 255, 80]}
                                        }
                                    },
                                    "width": 1.5
                                },
                                "description": (
                                    f"⚡ <b>High-Altitude Infrasound Shockwave Event</b><br/>"
                                    f"━━━━━━━━━━━━━━━━━━━━━<br/>"
                                    f"📅 Detonation Timestamp: {date_str} UTC<br/>"
                                    f"💥 Kinetic Yield Profile: {energy_kt} kt TNT equivalent<br/>"
                                    f"🎚️ Altitude Plane: {raw_alt if raw_alt else '35+'} km above surface<br/>"
                                    f"🚀 Entry Velocity: {velocity} km/s<br/>"
                                    f"🛰️ Source: US Gov Defense Satellite Sensor Matrix via NASA JPL"
                                )
                            }
                            
                            packets.append(packet)
                            mapped_count += 1
                            
                        except Exception:
                            continue
                            
                    print(f" [+] Success: Plotted {mapped_count} atmospheric shockwave profiles globally.")
                else:
                    print(f" [!] NASA API Connection Error: HTTP {resp.status_code}")
                    
        except Exception as e:
            print(f" [!] L36 Detonation pipeline connection breakdown: {e}")
            
        # Stream clean CZML package to your frontend architecture
        if len(packets) > 1:
            self._output_czml("atmospheric_fireballs", packets)
                

    async def ignite(self):
        print("\n=== STARTING GILU SINGLE-PASS HARVEST CYCLE ===")
        
        # Gathers all methods prefixed with 'l' and a number
        tasks = [
            getattr(self, method_name)() 
            for method_name in dir(self) 
            if re.match(r'^l\d+', method_name)
        ]
        
        print(f"[!] Processing {len(tasks)} distinct layers in parallel...")
        # Executes all 36 vector updates concurrently. Once the slowest layer completes, it unblocks.
        await asyncio.gather(*tasks)
        print("=== ALL GEOSPATIAL VECTOR LAYERS SYNCHRONIZED. EXITING DAEMON. ===")

if __name__ == "__main__":
    engine = GeoIntelligenceLayerUpdater()
    try:
        asyncio.run(engine.ignite())
    except KeyboardInterrupt:
        print("\n[!] OPERATOR ABORT. C2 ENGINE OFFLINE.")