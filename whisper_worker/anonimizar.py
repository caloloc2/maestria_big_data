"""Anonimización de transcripciones (Fase 3) — la frontera de privacidad.

Solo texto ANONIMIZADO puede salir hacia Gemini (nube). Combina Presidio +
spaCy ES con reconocedores propios del dominio ecuatoriano:
  - Cédula (10 dígitos, validación módulo 10),
  - Tarjeta (13-19 dígitos, validación Luhn),
  - Teléfonos Ecuador (móvil 09/9, fijo 0[2-7], +593),
  - Nombres propios (NER de spaCy ES),
  - Secuencias de NÚMEROS DICTADOS en palabras (caso PCI: tarjeta/CVV/cédula
    leídos en voz alta → Whisper los transcribe como "cinco nueve tres...").

Cada entidad se reemplaza por una etiqueta (<CEDULA>, <TARJETA>, <TELEFONO>,
<NOMBRE>, <DATO_NUMERICO>).
"""
import re

from presidio_analyzer import AnalyzerEngine, Pattern, PatternRecognizer
from presidio_analyzer.nlp_engine import NlpEngineProvider
from presidio_anonymizer import AnonymizerEngine
from presidio_anonymizer.entities import OperatorConfig

NUMS = {
    "cero", "uno", "una", "dos", "tres", "cuatro", "cinco", "seis", "siete",
    "ocho", "nueve", "diez", "once", "doce", "trece", "catorce", "quince",
    "dieciseis", "dieciséis", "veinte", "treinta", "cuarenta", "cincuenta",
    "sesenta", "setenta", "ochenta", "noventa", "cien", "ciento", "mil",
}


def validar_cedula(c: str) -> bool:
    if len(c) != 10 or not c.isdigit():
        return False
    prov = int(c[:2])
    if prov < 1 or (prov > 24 and prov != 30) or int(c[2]) >= 6:
        return False
    coef = [2, 1, 2, 1, 2, 1, 2, 1, 2]
    s = 0
    for i in range(9):
        p = int(c[i]) * coef[i]
        s += p - 9 if p > 9 else p
    return (10 - (s % 10)) % 10 == int(c[9])


def _luhn(digs: str) -> bool:
    s, alt = 0, False
    for ch in reversed(digs):
        d = int(ch)
        if alt:
            d = d * 2
            if d > 9:
                d -= 9
        s += d
        alt = not alt
    return s % 10 == 0


class _Cedula(PatternRecognizer):
    def __init__(self):
        super().__init__(supported_entity="EC_CEDULA", supported_language="es",
                         patterns=[Pattern("cedula", r"\b\d{10}\b", 0.3)])

    def validate_result(self, pattern_text):
        return validar_cedula(pattern_text)


class _Tarjeta(PatternRecognizer):
    def __init__(self):
        super().__init__(supported_entity="CREDIT_CARD", supported_language="es",
                         patterns=[Pattern("tarjeta", r"\b(?:\d[ -]?){13,19}\b", 0.3)])

    def validate_result(self, pattern_text):
        digs = re.sub(r"\D", "", pattern_text)
        return _luhn(digs) if 13 <= len(digs) <= 19 else False


def _build_analyzer():
    prov = NlpEngineProvider(nlp_configuration={
        "nlp_engine_name": "spacy",
        "models": [{"lang_code": "es", "model_name": "es_core_news_md"}],
    })
    az = AnalyzerEngine(nlp_engine=prov.create_engine(), supported_languages=["es"])
    az.registry.add_recognizer(_Cedula())
    az.registry.add_recognizer(_Tarjeta())
    az.registry.add_recognizer(PatternRecognizer(
        supported_entity="EC_PHONE", supported_language="es", patterns=[
            Pattern("movil", r"\b0?9\d{8}\b", 0.6),
            Pattern("fijo", r"\b0[2-7]\d{7}\b", 0.5),
            Pattern("intl", r"\+593\d{8,9}\b", 0.7),
        ]))
    return az


_analyzer = None
_anonymizer = AnonymizerEngine()
_OPS = {
    "EC_CEDULA": OperatorConfig("replace", {"new_value": "<CEDULA>"}),
    "CREDIT_CARD": OperatorConfig("replace", {"new_value": "<TARJETA>"}),
    "EC_PHONE": OperatorConfig("replace", {"new_value": "<TELEFONO>"}),
    "PHONE_NUMBER": OperatorConfig("replace", {"new_value": "<TELEFONO>"}),
    "PERSON": OperatorConfig("replace", {"new_value": "<NOMBRE>"}),
}
_ENTS = ["EC_CEDULA", "CREDIT_CARD", "EC_PHONE", "PHONE_NUMBER", "PERSON"]


def redactar_numeros_hablados(texto: str, minlen: int = 7) -> str:
    """Reemplaza secuencias de >=minlen números dictados en palabras (con hasta
    un hueco de 1 palabra) por <DATO_NUMERICO>."""
    words = texto.split()
    low = [w.strip(".,;:¿?¡!()\"'").lower() for w in words]
    isn = [w in NUMS for w in low]
    out, i, n = [], 0, len(words)
    while i < n:
        if isn[i]:
            last, k = i, i + 1
            while k < n and (isn[k] or k - last == 1):
                if isn[k]:
                    last = k
                k += 1
            if sum(isn[i:last + 1]) >= minlen:
                out.append("<DATO_NUMERICO>")
                i = last + 1
                continue
        out.append(words[i])
        i += 1
    return " ".join(out)


def anonimizar(texto: str) -> str:
    global _analyzer
    if _analyzer is None:
        _analyzer = _build_analyzer()
    texto = redactar_numeros_hablados(texto)
    res = _analyzer.analyze(text=texto, language="es", entities=_ENTS)
    return _anonymizer.anonymize(text=texto, analyzer_results=res, operators=_OPS).text
