# provisioning/menu.py
MENU = [
    # You can keep the path pointing to your entry script:
    {"label": "Landing Page", "path": "portfolio_homepage.py"},

    {"label": "ProvisionalAgent",
     "path": "provisionalagent_homepage.py",
     "children": [
         {"label": "Provision", "path": "pages/1_provision.py"},
     ]},

    {"label": "Admin", "path": None, "children": [
        {"label": "ProvisionAgent", "path": None, "children": [
            {"label": "Console",   "path": "pages/2_admin.py"},
            {"label": "Reports",   "path": "pages/3_Reports.py"},
            {"label": "Artifacts", "path": "pages/4_Artifacts.py"},
        ]},
    ]},

    {"label": "Logout", "path": "pages/9_Logout.py"},
]
