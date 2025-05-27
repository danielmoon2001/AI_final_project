# CIFAR-10 Robustness Experiments <- to fix 

This repository contains a deep learning mini-project that explores how different types of data and label corruptions affect image classification performance on the CIFAR-10 dataset.

Implemented using **PyTorch**, and designed for **Colab** execution.

---

## Project Overview

We evaluate two CNN architectures:

- `SimpleCNN`: A 2-layer baseline convolutional model
- `DeeperCNN`: A 4-layer model with BatchNorm and Dropout

Each model is tested under the following conditions:

1. Baseline (clean CIFAR-10)
2. Random Label Shuffle
3. Label Noise (20% corrupted labels)
4. Input Perturbation: Random Crop
5. Input Perturbation: Gaussian Blur

Results are evaluated using global accuracy, class-wise metrics, and confusion matrices.

---

## How to Run

This project is designed to run in a single pass in Google Colab.

1. Upload `CIFAR10_experimentation_project.ipynb` to Colab
2. Run all cells sequentially
3. Each experiment's results (accuracy + confusion matrix) will be printed

---

## Dependencies

See [`requirements.txt`](./requirements.txt) for all necessary packages.

---

## Author

- Created by **SeongYeon Moon** for an AI course mini-project
- 장병탁, 인공지능, Seoul National University, Spring 2025

---

## License

This project is for academic purposes only. All code is open for educational use.
