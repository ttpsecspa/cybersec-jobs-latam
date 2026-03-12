#!/usr/bin/env python3
"""
CyberSec Jobs LATAM - Crawler Profundo
Busca empleos de ciberseguridad en toda LATAM usando:
  1. Google Search (queries masivos por rol, país, plataforma, idioma)
  2. Scraping directo de plataformas de empleo LATAM
  3. APIs públicas de bolsas de trabajo

Diseñado para ejecutarse cada 3 horas via GitHub Actions.
"""

import json
import hashlib
import re
import time
import random
import logging
from datetime import datetime, timezone, timedelta
from pathlib import Path
from urllib.parse import quote_plus, urlparse, urlencode

import requests
from bs4 import BeautifulSoup

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).resolve().parent
DATA_FILE = BASE_DIR / "data" / "jobs.json"
HTML_FILE = BASE_DIR / "index.html"

LOG_FORMAT = "%(asctime)s [%(levelname)s] %(message)s"
logging.basicConfig(level=logging.INFO, format=LOG_FORMAT)
log = logging.getLogger("crawler")

# Rotar User-Agents para evitar bloqueos
USER_AGENTS = [
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/124.0.0.0 Safari/537.36",
    "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:125.0) Gecko/20100101 Firefox/125.0",
    "Mozilla/5.0 (Macintosh; Intel Mac OS X 10_15_7) AppleWebKit/605.1.15 (KHTML, like Gecko) Version/17.4 Safari/605.1.15",
]

MAX_JOBS = 500  # Máximo de empleos a mantener
MAX_AGE_DAYS = 45  # Eliminar empleos más viejos que esto

# ===========================================================================
# BÚSQUEDAS — El corazón del crawler
# ===========================================================================

# ---------- 20 países LATAM con ciudades principales ----------
LATAM_COUNTRIES = {
    "Chile": {
        "flag": "🇨🇱",
        "cities": ["Santiago", "Valparaíso", "Concepción", "Viña del Mar", "Antofagasta"],
        "domains": [".cl"],
        "platforms": ["computrabajo.cl", "laborum.cl", "chiletrabajos.cl", "trabajando.com"],
    },
    "Colombia": {
        "flag": "🇨🇴",
        "cities": ["Bogotá", "Medellín", "Cali", "Barranquilla", "Cartagena", "Bucaramanga"],
        "domains": [".co"],
        "platforms": ["computrabajo.com.co", "elempleo.com", "magneto365.com"],
    },
    "México": {
        "flag": "🇲🇽",
        "cities": ["CDMX", "Guadalajara", "Monterrey", "Puebla", "Querétaro", "Tijuana", "León", "Mérida"],
        "domains": [".mx"],
        "platforms": ["occ.com.mx", "computrabajo.com.mx", "indeed.com.mx", "empleo.gob.mx"],
    },
    "Argentina": {
        "flag": "🇦🇷",
        "cities": ["Buenos Aires", "Córdoba", "Rosario", "Mendoza", "La Plata", "Tucumán"],
        "domains": [".ar"],
        "platforms": ["bumeran.com.ar", "computrabajo.com.ar", "zonajobs.com.ar"],
    },
    "Perú": {
        "flag": "🇵🇪",
        "cities": ["Lima", "Arequipa", "Trujillo", "Cusco", "Chiclayo"],
        "domains": [".pe"],
        "platforms": ["computrabajo.com.pe", "bumeran.com.pe", "aptitus.com"],
    },
    "Brasil": {
        "flag": "🇧🇷",
        "cities": ["São Paulo", "Rio de Janeiro", "Brasília", "Belo Horizonte", "Curitiba", "Porto Alegre", "Recife", "Fortaleza", "Salvador"],
        "domains": [".br"],
        "platforms": ["vagas.com.br", "catho.com.br", "infojobs.com.br", "gupy.io", "trampos.co"],
    },
    "Costa Rica": {
        "flag": "🇨🇷",
        "cities": ["San José", "Heredia", "Alajuela", "Cartago"],
        "domains": [".cr"],
        "platforms": ["computrabajo.co.cr", "elempleo.co.cr"],
    },
    "Panamá": {
        "flag": "🇵🇦",
        "cities": ["Ciudad de Panamá", "David", "Colón"],
        "domains": [".pa"],
        "platforms": ["computrabajo.com.pa", "konzerta.com"],
    },
    "Uruguay": {
        "flag": "🇺🇾",
        "cities": ["Montevideo", "Punta del Este", "Salto"],
        "domains": [".uy"],
        "platforms": ["buscojobs.com.uy", "computrabajo.com.uy", "gallito.com.uy"],
    },
    "Ecuador": {
        "flag": "🇪🇨",
        "cities": ["Quito", "Guayaquil", "Cuenca", "Ambato"],
        "domains": [".ec"],
        "platforms": ["computrabajo.com.ec", "multitrabajos.com"],
    },
    "Rep. Dominicana": {
        "flag": "🇩🇴",
        "cities": ["Santo Domingo", "Santiago de los Caballeros", "Punta Cana"],
        "domains": [".do"],
        "platforms": ["computrabajo.com.do", "empleos.do"],
    },
    "Guatemala": {
        "flag": "🇬🇹",
        "cities": ["Ciudad de Guatemala", "Quetzaltenango", "Escuintla"],
        "domains": [".gt"],
        "platforms": ["computrabajo.com.gt", "tecoloco.com.gt"],
    },
    "Honduras": {
        "flag": "🇭🇳",
        "cities": ["Tegucigalpa", "San Pedro Sula"],
        "domains": [".hn"],
        "platforms": ["computrabajo.com.hn", "tecoloco.com.hn"],
    },
    "El Salvador": {
        "flag": "🇸🇻",
        "cities": ["San Salvador", "Santa Ana", "San Miguel"],
        "domains": [".sv"],
        "platforms": ["computrabajo.com.sv", "tecoloco.com.sv"],
    },
    "Paraguay": {
        "flag": "🇵🇾",
        "cities": ["Asunción", "Ciudad del Este", "Encarnación"],
        "domains": [".py"],
        "platforms": ["computrabajo.com.py", "paraguayempleo.com"],
    },
    "Bolivia": {
        "flag": "🇧🇴",
        "cities": ["La Paz", "Santa Cruz", "Cochabamba", "Sucre"],
        "domains": [".bo"],
        "platforms": ["computrabajo.com.bo"],
    },
    "Venezuela": {
        "flag": "🇻🇪",
        "cities": ["Caracas", "Maracaibo", "Valencia", "Barquisimeto"],
        "domains": [".ve"],
        "platforms": ["computrabajo.com.ve", "empleate.com"],
    },
    "Nicaragua": {
        "flag": "🇳🇮",
        "cities": ["Managua", "León", "Granada"],
        "domains": [".ni"],
        "platforms": ["computrabajo.com.ni", "tecoloco.com.ni"],
    },
    "Cuba": {
        "flag": "🇨🇺",
        "cities": ["La Habana", "Santiago de Cuba"],
        "domains": [".cu"],
        "platforms": [],
    },
    "Puerto Rico": {
        "flag": "🇵🇷",
        "cities": ["San Juan", "Bayamón", "Ponce"],
        "domains": [".pr"],
        "platforms": ["clasificadosonline.com"],
    },
}

# ---------- Roles de ciberseguridad (español + inglés + portugués) ----------
CYBER_ROLES_ES = [
    "Analista SOC", "Analista de Ciberseguridad", "Analista de Seguridad Informática",
    "Analista de Vulnerabilidades", "Analista de Ciberinteligencia", "Analista de Riesgos TI",
    "Ingeniero de Seguridad", "Ingeniero de Seguridad Cloud", "Ingeniero de Redes y Seguridad",
    "Ingeniero DevSecOps", "Ingeniero de Seguridad de Aplicaciones",
    "Pentester", "Ethical Hacker", "Red Team", "Blue Team", "Purple Team",
    "Consultor de Seguridad", "Consultor GRC", "Consultor ISO 27001",
    "Arquitecto de Seguridad", "Arquitecto de Ciberseguridad",
    "CISO", "Director de Seguridad", "Jefe de Seguridad Informática", "Líder de Ciberseguridad",
    "Especialista en Respuesta a Incidentes", "Especialista DFIR",
    "Especialista en Seguridad Cloud", "Especialista IAM",
    "Auditor de Seguridad", "Auditor TI", "Auditor de Sistemas",
    "Administrador de Seguridad", "Administrador de Firewalls",
    "Oficial de Seguridad de la Información", "ISSO",
    "Threat Hunter", "Threat Intelligence Analyst",
    "Analista de Malware", "Reverse Engineer",
    "Especialista en Continuidad de Negocio", "DRP Specialist",
    "Compliance Officer", "Data Protection Officer", "DPO",
    "Forense Digital", "Investigador Forense",
    "Instructor de Ciberseguridad", "Capacitador de Seguridad",
    "Security Champion", "Application Security Engineer",
    "Vulnerability Management Analyst", "Security Operations Center",
    "Incident Response Manager", "Security Awareness Specialist",
]

CYBER_ROLES_EN = [
    "SOC Analyst", "Cybersecurity Analyst", "Information Security Analyst",
    "Security Engineer", "Cloud Security Engineer", "Network Security Engineer",
    "DevSecOps Engineer", "Application Security Engineer", "AppSec",
    "Penetration Tester", "Ethical Hacker", "Red Team Operator",
    "Security Consultant", "GRC Consultant", "Risk Analyst",
    "Security Architect", "CISO", "Chief Information Security Officer",
    "Incident Response Analyst", "DFIR Analyst", "Forensic Analyst",
    "Threat Hunter", "Threat Intelligence Analyst", "CTI Analyst",
    "Malware Analyst", "Reverse Engineer", "Vulnerability Analyst",
    "IAM Engineer", "Identity Security Engineer",
    "Security Operations Manager", "SOC Manager", "SOC Lead",
    "Compliance Analyst", "Data Protection Officer",
    "Cloud Security Architect", "Zero Trust Engineer",
    "Blue Team Analyst", "Purple Team Engineer",
    "Security Automation Engineer", "SOAR Engineer",
    "OT Security Engineer", "ICS Security Specialist",
    "Cryptographer", "PKI Engineer",
]

CYBER_ROLES_PT = [
    "Analista de Segurança da Informação", "Analista SOC", "Analista de Cibersegurança",
    "Engenheiro de Segurança", "Engenheiro de Segurança Cloud",
    "Pentester", "Hacker Ético", "Red Team",
    "Consultor de Segurança", "Arquiteto de Segurança",
    "CISO", "Gerente de Segurança da Informação",
    "Especialista em Resposta a Incidentes", "Analista de Vulnerabilidades",
    "DevSecOps", "Engenheiro DevSecOps",
    "Analista de Threat Intelligence", "Threat Hunter",
    "Auditor de Segurança", "Oficial de Proteção de Dados", "DPO LGPD",
    "Forense Digital", "Perito em Computação Forense",
    "Analista de Governança de TI", "Analista GRC",
    "Engenheiro de Redes e Segurança",
    "Segurança de Aplicações", "AppSec Engineer",
]

# ---------- Plataformas globales de empleo tech ----------
GLOBAL_PLATFORMS = [
    "linkedin.com/jobs",
    "indeed.com",
    "glassdoor.com",
    "getonbrd.com",
    "torre.co",
    "remoteok.com",
    "weworkremotely.com",
    "angel.co/jobs",
    "hired.com",
    "stackoverflow.com/jobs",
    "infosec-jobs.com",
    "cybersecurityjobs.com",
    "careers.hackthebox.com",
    "securitycareers.help",
]

# ---------- Términos de búsqueda específicos de ciberseguridad ----------
CYBER_TERMS = [
    "ciberseguridad", "seguridad informática", "seguridad de la información",
    "cybersecurity", "information security", "infosec",
    "segurança da informação", "cibersegurança",  # Portugués
    "SOC", "SIEM", "pentesting", "ethical hacking",
    "GRC", "ISO 27001", "NIST", "compliance",
    "cloud security", "seguridad cloud",
    "DevSecOps", "AppSec", "application security",
    "incident response", "respuesta a incidentes",
    "threat intelligence", "threat hunting",
    "vulnerability management", "gestión de vulnerabilidades",
    "seguridad OT", "OT security", "ICS security",
    "forense digital", "digital forensics",
    "red team", "blue team", "purple team",
]

# ---------- Keywords de skills/requisitos ----------
SKILLS_KEYWORDS = [
    # Certificaciones
    "CISSP", "CEH", "OSCP", "OSCE", "OSWE", "OSEP", "CISA", "CISM",
    "CompTIA Security+", "CompTIA CySA+", "CompTIA CASP+", "CompTIA PenTest+",
    "GCIH", "GCIA", "GPEN", "GWAPT", "GCFE", "GNFA",
    "CCNA Security", "CCNP Security", "CCIE Security",
    "Azure Security Engineer", "AWS Security Specialty",
    "ISO 27001 Lead Auditor", "ISO 27001 Lead Implementer",
    "CRISC", "CGEIT", "CDPSE",
    "CCSP", "CCSK",
    "CRTP", "CRTO", "eCPPT", "eWPT", "eCPTX",
    "PNPT", "HTB CPTS", "HTB CDSA",
    # Frameworks / Estándares
    "ISO 27001", "ISO 27002", "ISO 27005", "ISO 22301",
    "NIST CSF", "NIST 800-53", "NIST 800-171",
    "PCI DSS", "SOC 2", "SOX", "HIPAA", "GDPR", "LGPD",
    "COBIT", "ITIL", "TOGAF", "SABSA",
    "MITRE ATT&CK", "MITRE D3FEND", "Cyber Kill Chain",
    "OWASP Top 10", "OWASP ASVS", "OWASP SAMM",
    "CIS Controls", "CIS Benchmarks",
    "Zero Trust", "ZTNA",
    # Herramientas
    "Splunk", "QRadar", "Sentinel", "ELK", "ArcSight", "LogRhythm",
    "CrowdStrike", "SentinelOne", "Carbon Black", "Cortex XDR",
    "Palo Alto", "Fortinet", "FortiGate", "Check Point",
    "Cisco ASA", "Cisco Firepower", "Meraki",
    "Nessus", "Qualys", "Rapid7", "Tenable", "OpenVAS",
    "Burp Suite", "Nmap", "Metasploit", "Cobalt Strike",
    "Wireshark", "Zeek", "Suricata", "Snort",
    "Volatility", "EnCase", "FTK", "Autopsy", "Cellebrite",
    "Maltego", "Shodan", "Censys", "VirusTotal",
    "Terraform", "Ansible", "Chef", "Puppet",
    "Docker", "Kubernetes", "AWS", "Azure", "GCP",
    "SOAR", "Phantom", "Demisto", "XSOAR",
    "Wazuh", "OSSEC", "Elastic Security",
    "HashiCorp Vault", "CyberArk", "BeyondTrust",
    # Lenguajes
    "Python", "PowerShell", "Bash", "Linux", "Go", "Rust",
    "JavaScript", "TypeScript", "Java", "C#", "C++",
    "SQL", "KQL", "SPL", "Regex",
    # Conceptos
    "SOC", "SIEM", "SOAR", "EDR", "XDR", "MDR", "NDR",
    "IAM", "PAM", "MFA", "SSO", "RBAC",
    "DLP", "CASB", "SASE", "SWG", "ZTNA",
    "WAF", "IDS", "IPS", "IDS/IPS",
    "Threat Intelligence", "OSINT", "CTI",
    "Incident Response", "DFIR", "Forensics",
    "Pentesting", "Pentest", "Ethical Hacking",
    "Red Team", "Blue Team", "Purple Team",
    "Vulnerability Management", "Patch Management",
    "Risk Management", "Risk Assessment",
    "Cloud Security", "Container Security",
    "DevSecOps", "CI/CD Security", "SAST", "DAST", "SCA",
    "Active Directory", "LDAP", "Kerberos",
    "VPN", "Firewall", "Proxy", "Load Balancer",
    "Encryption", "PKI", "TLS", "SSL",
    "GRC", "Compliance", "Auditoría", "Governance",
    "BCP", "DRP", "Business Continuity",
    "Security Awareness", "Phishing",
    "Malware Analysis", "Reverse Engineering",
    "OT Security", "SCADA", "ICS",
]

# ---------------------------------------------------------------------------
# Detección de país (expandido con más ciudades y keywords)
# ---------------------------------------------------------------------------
COUNTRY_KEYWORDS = {}
for country, info in LATAM_COUNTRIES.items():
    # Nombre del país
    COUNTRY_KEYWORDS[country.lower()] = country
    # Ciudades
    for city in info["cities"]:
        COUNTRY_KEYWORDS[city.lower()] = country
    # Dominios
    for dom in info["domains"]:
        COUNTRY_KEYWORDS[dom] = country
    # Plataformas
    for plat in info["platforms"]:
        COUNTRY_KEYWORDS[plat] = country

# Aliases adicionales
COUNTRY_KEYWORDS.update({
    "cdmx": "México", "ciudad de méxico": "México", "mexico city": "México",
    "bogota": "Colombia", "medellin": "Colombia",
    "sao paulo": "Brasil", "rio de janeiro": "Brasil",
    "san jose": "Costa Rica",
    "panama city": "Panamá",
    "santo domingo": "Rep. Dominicana", "dominican republic": "Rep. Dominicana",
    "costa rican": "Costa Rica",
    "guatemalteco": "Guatemala", "guatemala city": "Guatemala",
    "tegucigalpa": "Honduras", "san pedro sula": "Honduras",
    "san salvador": "El Salvador",
    "managua": "Nicaragua",
    "asuncion": "Paraguay", "ciudad del este": "Paraguay",
    "la paz": "Bolivia", "santa cruz": "Bolivia", "cochabamba": "Bolivia",
    "caracas": "Venezuela", "maracaibo": "Venezuela",
    "san juan": "Puerto Rico",
    "la habana": "Cuba", "havana": "Cuba",
})

REMOTE_KEYWORDS = [
    "remoto", "remote", "trabajo remoto", "home office", "teletrabajo",
    "100% remoto", "full remote", "anywhere", "work from home", "wfh",
    "remoto latam", "remote latam", "trabalho remoto",
]
HYBRID_KEYWORDS = ["híbrido", "hibrido", "hybrid", "semi-presencial", "semipresencial"]
ONSITE_KEYWORDS = ["presencial", "on-site", "onsite", "en oficina", "in-office", "presencialmente"]


# ===========================================================================
# GENERADOR DE QUERIES — Genera 150+ queries únicos
# ===========================================================================
def generate_search_queries() -> list:
    """Genera todas las combinaciones de búsquedas."""
    year = datetime.now().year
    queries = []

    # --- BLOQUE 1: Cada rol × cada país (español) ---
    # Seleccionar un subset de roles más comunes para no explotar en cantidad
    top_roles_es = [
        "Analista SOC", "Analista de Ciberseguridad", "Pentester",
        "Ingeniero de Seguridad", "Ingeniero DevSecOps", "CISO",
        "Consultor GRC", "Arquitecto de Seguridad", "Threat Hunter",
        "Especialista en Respuesta a Incidentes", "Analista de Vulnerabilidades",
        "Auditor de Seguridad", "Security Engineer", "Cloud Security",
        "Forense Digital", "Administrador de Firewalls", "Blue Team",
    ]
    top_countries = [
        "Chile", "Colombia", "México", "Argentina", "Perú", "Brasil",
        "Costa Rica", "Panamá", "Uruguay", "Ecuador", "Rep. Dominicana",
        "Guatemala", "Paraguay", "Bolivia", "Venezuela",
    ]

    for role in top_roles_es:
        for country in top_countries:
            queries.append(f'empleo "{role}" {country} {year}')

    # --- BLOQUE 2: Roles en inglés para LATAM remoto ---
    top_roles_en = [
        "SOC Analyst", "Cybersecurity Analyst", "Penetration Tester",
        "Security Engineer", "DevSecOps Engineer", "Cloud Security Engineer",
        "CISO", "GRC Consultant", "Threat Hunter", "DFIR Analyst",
        "Security Architect", "AppSec Engineer", "IAM Engineer",
        "Malware Analyst", "Vulnerability Analyst",
    ]
    for role in top_roles_en:
        queries.append(f'"{role}" LATAM remote {year}')
        queries.append(f'"{role}" Latin America remote {year}')

    # --- BLOQUE 3: Roles en portugués para Brasil ---
    for role in CYBER_ROLES_PT:
        queries.append(f'vaga "{role}" Brasil {year}')

    # --- BLOQUE 4: Búsquedas por plataforma (site:) ---
    platform_queries = {
        "linkedin.com/jobs": [
            "ciberseguridad", "cybersecurity", "seguridad informática",
            "SOC analyst", "pentester", "security engineer", "DevSecOps",
            "CISO", "GRC", "threat intelligence", "cloud security",
            "incident response", "vulnerability", "forense digital",
        ],
        "indeed.com": [
            "ciberseguridad latinoamerica", "cybersecurity latin america",
            "seguridad informática remoto", "SOC analyst LATAM",
        ],
        "getonbrd.com": [
            "seguridad", "ciberseguridad", "security", "DevSecOps",
            "pentesting", "SOC", "cloud security",
        ],
        "torre.co": [
            "ciberseguridad", "security engineer", "SOC analyst",
            "pentester", "DevSecOps",
        ],
        "computrabajo.com": [
            "ciberseguridad", "seguridad informática", "seguridad de la información",
            "SOC", "pentesting", "hacking ético", "firewall",
        ],
        "occ.com.mx": [
            "ciberseguridad", "seguridad informática", "ethical hacking",
        ],
        "bumeran.com": [
            "ciberseguridad", "seguridad informática", "SOC",
        ],
        "elempleo.com": [
            "ciberseguridad", "seguridad informática",
        ],
        "infosec-jobs.com": [
            "latin america", "LATAM", "remote",
        ],
        "remoteok.com": [
            "cybersecurity", "security engineer", "SOC",
        ],
        "weworkremotely.com": [
            "security", "cybersecurity", "infosec",
        ],
        "vagas.com.br": [
            "segurança da informação", "cibersegurança", "SOC", "pentester",
        ],
        "catho.com.br": [
            "segurança da informação", "cibersegurança", "analista SOC",
        ],
        "gupy.io": [
            "segurança da informação", "cibersegurança", "security",
        ],
    }

    for site, terms in platform_queries.items():
        for term in terms:
            queries.append(f'site:{site} {term}')

    # --- BLOQUE 5: Búsquedas genéricas con variaciones ---
    generic_templates = [
        'vacante ciberseguridad {country} {year}',
        'oferta laboral seguridad informática {country} {year}',
        '"se busca" ciberseguridad {country}',
        'hiring cybersecurity {country} {year}',
        'empleo "seguridad de la información" {country}',
        'trabajo "ethical hacking" {country}',
        'empleo "seguridad cloud" {country} {year}',
        'vacante "SOC analyst" {country}',
        'empleo "DevSecOps" {country}',
        'empleo "incident response" {country}',
        'trabajo "threat intelligence" {country}',
    ]

    for template in generic_templates:
        for country in ["Chile", "Colombia", "México", "Argentina", "Perú", "Brasil"]:
            queries.append(template.format(country=country, year=year))

    # --- BLOQUE 6: Búsquedas por ciudad principal ---
    city_searches = [
        ("Santiago", "Chile"), ("Bogotá", "Colombia"), ("CDMX", "México"),
        ("Buenos Aires", "Argentina"), ("Lima", "Perú"), ("São Paulo", "Brasil"),
        ("Guadalajara", "México"), ("Monterrey", "México"), ("Medellín", "Colombia"),
        ("Córdoba", "Argentina"), ("Rio de Janeiro", "Brasil"), ("Curitiba", "Brasil"),
        ("San José", "Costa Rica"), ("Ciudad de Panamá", "Panamá"),
        ("Montevideo", "Uruguay"), ("Quito", "Ecuador"), ("Guayaquil", "Ecuador"),
        ("Santo Domingo", "Rep. Dominicana"), ("Ciudad de Guatemala", "Guatemala"),
    ]
    for city, country in city_searches:
        queries.append(f'empleo ciberseguridad "{city}" {year}')
        queries.append(f'cybersecurity job "{city}" {year}')

    # --- BLOQUE 7: Remoto LATAM ---
    remote_terms = [
        'empleo ciberseguridad remoto latam {year}',
        'trabajo remoto seguridad informática latinoamerica {year}',
        'cybersecurity remote LATAM {year}',
        'remote cybersecurity job latin america {year}',
        '"100% remoto" ciberseguridad latam',
        '"full remote" cybersecurity LATAM',
        'teletrabajo ciberseguridad latam {year}',
        'home office seguridad informática {year}',
        '"trabalho remoto" "segurança da informação" {year}',
        '"trabalho remoto" cibersegurança Brasil {year}',
        'empleo remoto SOC analyst latam {year}',
        'empleo remoto pentester latam {year}',
        'empleo remoto DevSecOps latam {year}',
        'remote security engineer latin america {year}',
    ]
    for q in remote_terms:
        queries.append(q.format(year=year))

    # --- BLOQUE 8: Certificaciones específicas ---
    cert_queries = [
        'empleo CISSP latam {year}',
        'empleo OSCP latam {year}',
        'empleo CEH latam {year}',
        'empleo "CompTIA Security+" latam {year}',
        'empleo CISA latam {year}',
        'empleo "ISO 27001" auditor latam {year}',
        'vaga CISSP Brasil {year}',
        'hiring OSCP latin america {year}',
    ]
    for q in cert_queries:
        queries.append(q.format(year=year))

    # Deduplicar
    seen = set()
    unique = []
    for q in queries:
        normalized = q.lower().strip()
        if normalized not in seen:
            seen.add(normalized)
            unique.append(q)

    log.info(f"Total queries generados: {len(unique)}")
    return unique


# ===========================================================================
# SCRAPER DE GOOGLE
# ===========================================================================
def get_headers():
    """Retorna headers con User-Agent aleatorio."""
    return {
        "User-Agent": random.choice(USER_AGENTS),
        "Accept-Language": "es-419,es;q=0.9,en;q=0.8,pt;q=0.7",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "DNT": "1",
    }


def search_google(query: str, num_results: int = 15, start: int = 0) -> list:
    """Busca en Google y retorna lista de resultados con paginación."""
    results = []
    params = {
        "q": query,
        "num": num_results,
        "start": start,
        "hl": "es",
        "gl": "cl",  # Geolocalización Chile/LATAM
    }
    url = f"https://www.google.com/search?{urlencode(params)}"

    try:
        resp = requests.get(url, headers=get_headers(), timeout=20)
        if resp.status_code == 429:
            log.warning("Google rate limit (429). Esperando 60s...")
            time.sleep(60)
            resp = requests.get(url, headers=get_headers(), timeout=20)
        resp.raise_for_status()
    except requests.RequestException as e:
        log.warning(f"Error buscando '{query[:60]}...': {e}")
        return results

    soup = BeautifulSoup(resp.text, "lxml")

    for g in soup.select("div.g"):
        link_el = g.select_one("a[href]")
        title_el = g.select_one("h3")
        if not link_el or not title_el:
            continue

        href = link_el.get("href", "")
        title = title_el.get_text(strip=True)

        # Extraer snippet - intentar múltiples selectores
        snippet = ""
        for sel in ["div.VwiC3b", "span.aCOpRe", "div[data-sncf]", "div.IsZvec"]:
            el = g.select_one(sel)
            if el:
                snippet = el.get_text(strip=True)
                break

        if href.startswith("/url?q="):
            href = href.split("/url?q=")[1].split("&")[0]
        if not href.startswith("http"):
            continue

        domain = urlparse(href).netloc.lower()
        skip = ["google.com", "youtube.com", "facebook.com", "twitter.com",
                "instagram.com", "tiktok.com", "pinterest.com", "wikipedia.org"]
        if any(d in domain for d in skip):
            continue

        results.append({"title": title, "url": href, "snippet": snippet})

    return results


def search_google_with_pagination(query: str, max_pages: int = 2) -> list:
    """Busca en Google con paginación para obtener más resultados."""
    all_results = []
    for page in range(max_pages):
        start = page * 15
        results = search_google(query, num_results=15, start=start)
        all_results.extend(results)
        if len(results) < 10:
            break  # No más resultados
        if page < max_pages - 1:
            time.sleep(random.uniform(2, 5))
    return all_results


# ===========================================================================
# SCRAPERS DIRECTOS DE PLATAFORMAS
# ===========================================================================
def scrape_getonbrd() -> list:
    """Scraping directo de GetOnBrd (plataforma tech LATAM)."""
    results = []
    categories = ["security", "cybersecurity", "devsecops", "devops"]

    for cat in categories:
        url = f"https://www.getonbrd.com/jobs/{cat}"
        try:
            resp = requests.get(url, headers=get_headers(), timeout=15)
            resp.raise_for_status()
            soup = BeautifulSoup(resp.text, "lxml")

            for card in soup.select("div[data-item-id], a.gb-results-list__item"):
                title_el = card.select_one("h3, strong.gb-results-list__title")
                link_el = card.select_one("a[href]") if card.name != "a" else card
                company_el = card.select_one("span.gb-results-list__company, div.company")
                location_el = card.select_one("span.gb-results-list__location, div.location")

                if not title_el:
                    continue

                title = title_el.get_text(strip=True)
                href = link_el.get("href", "") if link_el else ""
                if href and not href.startswith("http"):
                    href = f"https://www.getonbrd.com{href}"

                company = company_el.get_text(strip=True) if company_el else "GetOnBrd"
                location = location_el.get_text(strip=True) if location_el else ""

                results.append({
                    "title": title,
                    "url": href,
                    "snippet": f"{company} - {location}",
                    "source": "getonbrd.com",
                })

            time.sleep(random.uniform(2, 4))
        except Exception as e:
            log.warning(f"Error scrapeando GetOnBrd/{cat}: {e}")

    log.info(f"GetOnBrd: {len(results)} resultados")
    return results


def scrape_torre() -> list:
    """Scraping de Torre.co via su API pública."""
    results = []
    search_terms = [
        "cybersecurity", "ciberseguridad", "security engineer",
        "SOC analyst", "pentester", "DevSecOps", "information security",
    ]

    for term in search_terms:
        url = "https://torre.co/api/suite/opportunities/search"
        payload = {
            "currency": "USD$",
            "page": 0,
            "periodicity": "monthly",
            "lang": "es",
            "size": 20,
            "aggregate": False,
            "offset": 0,
        }

        try:
            resp = requests.post(
                url,
                json=payload,
                headers={**get_headers(), "Content-Type": "application/json"},
                params={"keyword": term},
                timeout=15,
            )
            if resp.status_code == 200:
                data = resp.json()
                for item in data.get("results", []):
                    opp = item.get("objective", "") or item.get("objectiveTitle", "")
                    org = item.get("organizations", [{}])
                    company = org[0].get("name", "Torre.co") if org else "Torre.co"
                    locations = item.get("locations", [])
                    loc_str = ", ".join(locations) if locations else "LATAM"
                    opp_id = item.get("id", "")

                    results.append({
                        "title": opp,
                        "url": f"https://torre.co/jobs/{opp_id}" if opp_id else "https://torre.co",
                        "snippet": f"{company} - {loc_str}",
                        "source": "torre.co",
                    })

            time.sleep(random.uniform(1, 3))
        except Exception as e:
            log.warning(f"Error en Torre.co ({term}): {e}")

    log.info(f"Torre.co: {len(results)} resultados")
    return results


def scrape_remoteok() -> list:
    """Scraping de RemoteOK via API JSON."""
    results = []
    url = "https://remoteok.com/api?tag=security"

    try:
        resp = requests.get(url, headers=get_headers(), timeout=15)
        if resp.status_code == 200:
            data = resp.json()
            for item in data:
                if not isinstance(item, dict) or "position" not in item:
                    continue

                title = item.get("position", "")
                company = item.get("company", "RemoteOK")
                location = item.get("location", "Remote")
                job_url = item.get("url", "")
                tags = item.get("tags", [])

                # Filtrar solo si parece ser cybersecurity
                text = f"{title} {' '.join(tags)}".lower()
                cyber_words = ["security", "cybersec", "soc", "pentest", "devsecops",
                               "infosec", "threat", "incident", "vulnerability", "forensic"]
                if not any(w in text for w in cyber_words):
                    continue

                results.append({
                    "title": title,
                    "url": job_url if job_url.startswith("http") else f"https://remoteok.com{job_url}",
                    "snippet": f"{company} - {location} - {', '.join(tags[:5])}",
                    "source": "remoteok.com",
                })
    except Exception as e:
        log.warning(f"Error en RemoteOK: {e}")

    log.info(f"RemoteOK: {len(results)} resultados")
    return results


# ===========================================================================
# PROCESAMIENTO DE RESULTADOS
# ===========================================================================
def make_id(title: str, url: str) -> str:
    raw = f"{title.lower().strip()}|{url.strip()}"
    return hashlib.md5(raw.encode()).hexdigest()[:12]


def detect_country(text: str) -> str:
    text_lower = text.lower()
    # Buscar coincidencias más específicas primero (ciudades antes que países genéricos)
    best_match = None
    best_len = 0
    for keyword, country in COUNTRY_KEYWORDS.items():
        if keyword in text_lower and len(keyword) > best_len:
            best_match = country
            best_len = len(keyword)
    return best_match or "LATAM"


def detect_modality(text: str) -> str:
    text_lower = text.lower()
    for kw in REMOTE_KEYWORDS:
        if kw in text_lower:
            return "Remoto"
    for kw in HYBRID_KEYWORDS:
        if kw in text_lower:
            return "Híbrido"
    for kw in ONSITE_KEYWORDS:
        if kw in text_lower:
            return "Presencial"
    return "No especificado"


def detect_skills(text: str) -> list:
    found = []
    text_lower = text.lower()
    for skill in SKILLS_KEYWORDS:
        if skill.lower() in text_lower and skill not in found:
            found.append(skill)
    return found[:10]


def detect_city(text: str, country: str) -> str:
    text_lower = text.lower()
    info = LATAM_COUNTRIES.get(country)
    if info:
        for city in info["cities"]:
            if city.lower() in text_lower:
                return city
    return country


def get_flag(country: str) -> str:
    info = LATAM_COUNTRIES.get(country)
    if info:
        return info["flag"]
    return "🌎"


def clean_title(title: str) -> str:
    patterns = [
        r'\s*[-–|·]\s*(LinkedIn|Indeed|Computrabajo|Bumeran|GetOnBrd|Torre|Glassdoor|OCC|ZonaJobs|InfoJobs|Catho|Vagas|Gupy).*$',
        r'\s*[-–|·]\s*\w+\.\w{2,3}(\.\w{2})?.*$',
    ]
    cleaned = title
    for p in patterns:
        cleaned = re.sub(p, '', cleaned, flags=re.IGNORECASE)
    return cleaned.strip()


def is_job_related(title: str, snippet: str) -> bool:
    text = f"{title} {snippet}".lower()
    job_indicators = [
        # Español
        "empleo", "trabajo", "vacante", "oferta laboral", "convocatoria",
        "postula", "aplica", "buscamos", "se busca", "se requiere",
        "incorporar", "contratar", "selección", "puesto",
        # Inglés
        "hiring", "job", "position", "apply", "career", "opportunity",
        "looking for", "we are hiring", "join our team", "opening",
        # Portugués
        "vaga", "oportunidade", "contratando", "candidatar",
        # Roles (cualquier idioma)
        "analyst", "analista", "engineer", "ingeniero", "engenheiro",
        "consultant", "consultor", "specialist", "especialista",
        "senior", "junior", "lead", "líder", "manager", "gerente",
        "director", "architect", "arquitecto", "arquiteto",
        "pentester", "devsecops", "soc", "ciso", "ethical hacker",
        "auditor", "administrator", "administrador",
    ]
    return any(ind in text for ind in job_indicators)


def extract_company(title: str, snippet: str, url: str) -> str:
    """Intenta extraer el nombre de la empresa."""
    # Patrones en snippet
    patterns = [
        r'(?:en|at|@|para|company:)\s+([A-ZÁ-Ú][A-Za-záéíóúñÁÉÍÓÚÑ\s&.]+?)(?:\s*[-–|,.]|\s+(?:busca|requiere|necesita|está))',
        r'^([A-ZÁ-Ú][A-Za-záéíóúñÁÉÍÓÚÑ\s&.]{2,30}?)(?:\s*[-–|])',
    ]
    for p in patterns:
        m = re.search(p, snippet)
        if m:
            company = m.group(1).strip()
            if len(company) > 3 and len(company) < 50:
                return company

    # Extraer del dominio
    domain = urlparse(url).netloc.replace("www.", "")
    # Si es una plataforma de empleo, no usar como empresa
    job_platforms = [
        "linkedin.com", "indeed.com", "glassdoor.com", "computrabajo",
        "bumeran.com", "occ.com", "getonbrd.com", "torre.co",
        "zonajobs.com", "elempleo.com", "infojobs.com", "catho.com",
        "vagas.com", "gupy.io", "remoteok.com", "weworkremotely.com",
    ]
    if not any(p in domain for p in job_platforms):
        name = domain.split(".")[0]
        return name.replace("-", " ").title()

    return "Ver en oferta"


def process_result(result: dict) -> dict | None:
    title = result["title"]
    url = result["url"]
    snippet = result.get("snippet", "")
    source = result.get("source", urlparse(url).netloc.replace("www.", ""))
    combined = f"{title} {snippet} {url}"

    if not is_job_related(title, snippet):
        return None

    cleaned_title = clean_title(title)
    if len(cleaned_title) < 5:
        return None

    country = detect_country(combined)
    city = detect_city(combined, country)
    modality = detect_modality(combined)
    skills = detect_skills(combined)
    company = extract_company(title, snippet, url)

    return {
        "id": make_id(cleaned_title, url),
        "title": cleaned_title,
        "company": company,
        "country": country,
        "flag": get_flag(country),
        "city": city,
        "modality": modality,
        "requirements": skills if skills else ["Ciberseguridad"],
        "date": datetime.now(timezone.utc).strftime("%Y-%m-%d"),
        "url": url,
        "salary": "",
        "source": source,
        "found_at": datetime.now(timezone.utc).isoformat(),
    }


# ===========================================================================
# DATA MANAGEMENT
# ===========================================================================
def load_jobs() -> dict:
    if DATA_FILE.exists():
        with open(DATA_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    return {"metadata": {"last_updated": "", "total_jobs": 0, "sources": []}, "jobs": []}


def save_jobs(data: dict):
    DATA_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(DATA_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)
    log.info(f"Guardados {len(data['jobs'])} empleos en {DATA_FILE}")


def update_html(jobs: list):
    if not HTML_FILE.exists():
        log.warning(f"No se encontró {HTML_FILE}")
        return

    with open(HTML_FILE, "r", encoding="utf-8") as f:
        html = f.read()

    js_entries = []
    for job in jobs:
        requirements_js = json.dumps(job.get("requirements", []), ensure_ascii=False)
        entry = (
            "        {\n"
            f'            title: {json.dumps(job["title"], ensure_ascii=False)},\n'
            f'            company: {json.dumps(job.get("company", ""), ensure_ascii=False)},\n'
            f'            country: {json.dumps(job.get("country", "LATAM"), ensure_ascii=False)},\n'
            f'            flag: {json.dumps(job.get("flag", "🌎"), ensure_ascii=False)},\n'
            f'            city: {json.dumps(job.get("city", ""), ensure_ascii=False)},\n'
            f'            modality: {json.dumps(job.get("modality", "No especificado"), ensure_ascii=False)},\n'
            f'            requirements: {requirements_js},\n'
            f'            date: {json.dumps(job.get("date", ""), ensure_ascii=False)},\n'
            f'            url: {json.dumps(job.get("url", ""), ensure_ascii=False)},\n'
            f'            salary: {json.dumps(job.get("salary", ""), ensure_ascii=False)}\n'
            "        }"
        )
        js_entries.append(entry)

    js_array = "[\n" + ",\n".join(js_entries) + "\n    ]"

    pattern = r'const JOBS_DATA = \[.*?\];'
    replacement = f'const JOBS_DATA = {js_array};'
    new_html = re.sub(pattern, replacement, html, flags=re.DOTALL)

    with open(HTML_FILE, "w", encoding="utf-8") as f:
        f.write(new_html)
    log.info(f"HTML actualizado con {len(jobs)} empleos")


def merge_jobs(existing: list, new_jobs: list) -> list:
    by_id = {}
    for job in existing:
        by_id[job["id"]] = job

    added = 0
    for job in new_jobs:
        if job["id"] not in by_id:
            by_id[job["id"]] = job
            added += 1

    log.info(f"{added} empleos nuevos, {len(new_jobs) - added} duplicados omitidos")

    # Filtrar empleos viejos
    cutoff = (datetime.now(timezone.utc) - timedelta(days=MAX_AGE_DAYS)).strftime("%Y-%m-%d")
    all_jobs = [j for j in by_id.values() if j.get("date", "") >= cutoff]

    # Ordenar por fecha descendente
    all_jobs.sort(key=lambda j: j.get("date", ""), reverse=True)

    return all_jobs[:MAX_JOBS]


# ===========================================================================
# MAIN — Orquestador
# ===========================================================================
def run():
    log.info("=" * 70)
    log.info("  CyberSec Jobs LATAM - Crawler Profundo")
    log.info("=" * 70)

    data = load_jobs()
    existing_jobs = data.get("jobs", [])
    log.info(f"Empleos existentes en base de datos: {len(existing_jobs)}")

    all_new = []
    total_google = 0
    total_direct = 0

    # ---- FASE 1: Scraping directo de plataformas ----
    log.info("\n--- FASE 1: Scraping directo de plataformas ---")
    try:
        direct_results = []
        direct_results.extend(scrape_getonbrd())
        direct_results.extend(scrape_torre())
        direct_results.extend(scrape_remoteok())

        for r in direct_results:
            job = process_result(r)
            if job:
                all_new.append(job)
                total_direct += 1
    except Exception as e:
        log.error(f"Error en scraping directo: {e}")

    log.info(f"Fase 1 completada: {total_direct} empleos de plataformas directas")

    # ---- FASE 2: Google Search masivo ----
    log.info("\n--- FASE 2: Google Search masivo ---")
    queries = generate_search_queries()

    # Limitar queries por ejecución para no abusar de Google
    # En cada run de 3 horas, ejecutar un batch diferente
    batch_size = 60  # ~60 queries por ejecución
    hour = datetime.now().hour
    batch_index = (hour // 3) % max(1, len(queries) // batch_size)
    batch_start = batch_index * batch_size
    batch_queries = queries[batch_start:batch_start + batch_size]

    # Si sobran queries del final, agregar del inicio
    if len(batch_queries) < batch_size:
        remaining = batch_size - len(batch_queries)
        batch_queries.extend(queries[:remaining])

    log.info(f"Ejecutando batch {batch_index + 1}: queries {batch_start + 1}-{batch_start + len(batch_queries)} de {len(queries)}")

    for i, query in enumerate(batch_queries):
        log.info(f"[{i+1}/{len(batch_queries)}] {query[:80]}...")

        results = search_google(query, num_results=15)
        total_google += len(results)

        for r in results:
            job = process_result(r)
            if job:
                all_new.append(job)

        # Pausa inteligente
        delay = random.uniform(4, 10)
        if (i + 1) % 10 == 0:
            delay = random.uniform(15, 30)  # Pausa más larga cada 10 queries
            log.info(f"  Pausa larga: {delay:.0f}s")
        time.sleep(delay)

    log.info(f"\nFase 2 completada: {total_google} resultados de Google")

    # ---- FASE 3: Merge y guardado ----
    log.info("\n--- FASE 3: Merge y guardado ---")
    log.info(f"Empleos válidos encontrados en total: {len(all_new)}")

    merged = merge_jobs(existing_jobs, all_new)

    # Estadísticas
    countries = {}
    modalities = {}
    sources = set()
    for j in merged:
        c = j.get("country", "LATAM")
        m = j.get("modality", "N/A")
        countries[c] = countries.get(c, 0) + 1
        modalities[m] = modalities.get(m, 0) + 1
        sources.add(j.get("source", "unknown"))

    data["jobs"] = merged
    data["metadata"] = {
        "last_updated": datetime.now(timezone.utc).isoformat(),
        "total_jobs": len(merged),
        "sources": sorted(sources),
        "countries": countries,
        "modalities": modalities,
        "batch_index": batch_index,
        "total_queries": len(queries),
    }
    save_jobs(data)

    # Resumen final
    log.info("\n" + "=" * 70)
    log.info("  RESUMEN")
    log.info("=" * 70)
    log.info(f"Total empleos en BD: {len(merged)}")
    log.info(f"Fuentes únicas: {len(sources)}")
    log.info(f"Por país:")
    for c, n in sorted(countries.items(), key=lambda x: -x[1]):
        log.info(f"  {get_flag(c)} {c}: {n}")
    log.info(f"Por modalidad:")
    for m, n in sorted(modalities.items(), key=lambda x: -x[1]):
        log.info(f"  {m}: {n}")
    log.info("Crawler finalizado exitosamente")


if __name__ == "__main__":
    run()
