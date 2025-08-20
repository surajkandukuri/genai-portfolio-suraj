# provisioning/menu.py
# Edit labels/paths here and the sidebar updates everywhere.

MENU = [
    {"label": "Home",      "path": "provisionalagent_homepage.py"},
    {"label": "Provision", "path": "pages/1_provision.py"},   # match your actual filename
    {
      "label": "Admin", "path": None,  # section header, no direct page
      "children": [
          {"label": "Reports",   "path": "pages/3_Reports.py"},
          {"label": "Console",   "path": "pages/2_admin.py"},  # note: lowercase 'admin' per your file
          {"label": "Artifacts", "path": "pages/4_Artifacts.py"},
      ],
    },
    # {"label": "Account",   "path": "pages/8_Account.py"},   # uncomment if/when you add it
    {"label": "Logout",    "path": "pages/9_Logout.py"},
]
