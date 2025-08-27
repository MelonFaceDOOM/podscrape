# Centralized ASR loader + runners for faster-whisper (fw_*) and openai-whisper (oa_*)

import os, site, glob
from functools import partial

# --- keep OpenMP from crashing on Windows when torch/ctranslate2 both bring libiomp ---
os.environ.setdefault("KMP_DUPLICATE_LIB_OK", "TRUE")
os.environ.setdefault("OMP_NUM_THREADS", "1")

# add ffmpeg & NVIDIA user-space runtimes to PATH/DLL search (Windows-safe) ---
FFMPEG_DIR = os.getenv("FFMPEG_DIR")  # e.g., C:/ffmpeg/bin
if FFMPEG_DIR and os.path.isdir(FFMPEG_DIR):
    os.environ["PATH"] = FFMPEG_DIR + os.pathsep + os.environ.get("PATH", "")

def _add_runtime_paths():
    # Bring pip-installed CUDA/cuDNN wheels into DLL search path (Windows)
    try:
        for base in site.getsitepackages():
            for pat in ("nvidia/*/bin", "nvidia/*/lib", "nvidia/*/lib/*"):
                for p in glob.glob(os.path.join(base, pat)):
                    if os.name == "nt":
                        try:
                            os.add_dll_directory(p)  # Python 3.8+
                        except Exception:
                            pass
    except Exception:
        pass

_add_runtime_paths()

# --- Import frameworks lazily to avoid loading both when not needed ---
import importlib

def _import_whisper():
    return importlib.import_module("whisper")  # openai-whisper

def _import_fw():
    return importlib.import_module("faster_whisper")


# ---------------- runners ----------------

def oa_text_segments(model, mp3_path):
    r = model.transcribe(str(mp3_path), word_timestamps=False)
    return [(s['start'], s['end'], s['text']) for s in r['segments']]

def oa_text_segments_word_level(model, mp3_path):
    r = model.transcribe(str(mp3_path), word_timestamps=True)
    seg_rows, word_rows = [], []
    for seg_idx, seg in enumerate(r["segments"]):
        seg_rows.append((seg["start"], seg["end"], seg["text"]))
        for word_idx, w in enumerate(seg.get("words", []) or []):
            word_rows.append((seg_idx, word_idx, w["start"], w["end"], w["word"]))
    return seg_rows, word_rows

def fw_text_segments(model, mp3_path):
    seg_iter, _ = model.transcribe(str(mp3_path), beam_size=1)
    return [(s.start, s.end, s.text) for s in seg_iter]

def fw_text_segments_word_level(model, mp3_path):
    seg_iter, _ = model.transcribe(str(mp3_path), beam_size=1, word_timestamps=True)
    seg_rows, word_rows = [], []
    for seg_idx, seg in enumerate(seg_iter):
        seg_rows.append((seg.start, seg.end, seg.text))
        for word_idx, w in enumerate(seg.words or []):
            word_rows.append((seg_idx, word_idx, w.start, w.end, w.word))
    return seg_rows, word_rows


# ---------------- registry ----------------

MODELS = {
    # OpenAI-whisper (CPU or CUDA; installs torchaudio/ffmpeg deps)
    "oa_base": dict(
        build=lambda device="cuda": _import_whisper().load_model("base", device=device),
        seg_runner=oa_text_segments,
        word_runner=oa_text_segments_word_level,
    ),
    # faster-whisper (CTranslate2)
    "fw_base": dict(
        build=lambda device="cuda": _import_fw().WhisperModel("base", device=device, compute_type="float16"),
        seg_runner=fw_text_segments,
        word_runner=fw_text_segments_word_level,
    ),
    "fw_tiny": dict(
        build=lambda device="cuda": _import_fw().WhisperModel("tiny.en", device=device, compute_type="float16"),
        seg_runner=fw_text_segments,
        word_runner=fw_text_segments_word_level,
    ),
}

_loaded = {}

def get_model(name: str, device: str = "cuda"):
    """Return (name, model, seg_runner)."""
    if name not in MODELS:
        raise ValueError(f"Unknown model name: {name}")
    if name not in _loaded:
        _loaded[name] = MODELS[name]["build"](device=device)
    return name, _loaded[name], MODELS[name]["seg_runner"]

def get_word_level_model(name: str, device: str = "cuda"):
    """Return (model, word_runner)."""
    if name not in MODELS:
        raise ValueError(f"Unknown model name: {name}")
    if name not in _loaded:
        _loaded[name] = MODELS[name]["build"](device=device)
    return _loaded[name], MODELS[name]["word_runner"]