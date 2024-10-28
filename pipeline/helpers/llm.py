"""
Helper functions for interacting with the LLM models using ollama.
"""

import logging
from typing import Any, Dict, cast

import ollama

logger = logging.getLogger(__name__)

MODEL_PULLED = False


def prompt_llm(
    prompt: str,
    model: str,
) -> Dict[str, Any]:
    """
    Prompt the LLM model, using ollama.

    Args:
        prompt (str): The prompt for the LLM model.
        model (str): The model to use.

    Returns:
        str: The response from the LLM model.
    """
    global MODEL_PULLED  # pylint: disable=global-statement

    if not MODEL_PULLED:
        logger.info(f"Pulling model: ollama:{model}")
        # Ensure model is available
        response = ollama.pull(model)

        if response["status"] != "success":  # type: ignore
            logger.error(f"ollama: pulling {model} failed")
            raise ValueError(f"ollama: pulling {model} failed")

        MODEL_PULLED = True
        logger.info(f"Model ollama:{model} pulled")

    response = ollama.chat(
        model=model,
        messages=[
            {
                "role": "user",
                "content": prompt,
            }
        ],
        stream=False,
    )

    dict_response: Dict[str, Any] = cast(Dict[str, Any], response)
    return dict_response
