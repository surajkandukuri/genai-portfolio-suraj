# provisioning/menu.py
MENU = [
    {"label": "Landing Page", "path": "portfolio_homepage.py"},

    {"label": "ProvisionalAgent", "path": "provisionalagent_homepage.py",
     "children": [
        {"label": "Provision", "path": "pages/1_provision.py"},
     ]},

    
    {"label": "KPI Drift Hunter Agent", "path": None, "children": [
        {"label": "KPI Drift Hunter", "path": "kpidrifthunteragent_homepage.py"},   # homepage
        {"label": "Run the Scan",     "path": "pages/21_kpidrift_runthescan.py"},    # scan page
        {"label": "Widget Extractor", "path": "pages/22_kpidrift_widgetextractor.py"}# new extractor
    ]},

    {"label": "Admin · ProvisionalAgent", "path": None, "children": [
        {"label": "Console",   "path": "pages/2_admin.py"},
        {"label": "Reports",   "path": "pages/3_Reports.py"},
        {"label": "Artifacts", "path": "pages/4_Artifacts.py"},
    ]},

    {"label": "Logout", "path": "pages/9_Logout.py"},
]
