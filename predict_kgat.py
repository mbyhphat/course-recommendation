import sys
import random
from time import time

import pandas as pd
from tqdm import tqdm
import torch.nn as nn
import torch.optim as optim

from model.KGAT import KGAT
from parser.parser_kgat import *
from utils.log_helper import *
from utils.metrics import *
from utils.model_helper import *
from data_loader.loader_kgat import DataLoaderKGAT


def evaluate(
    model,
    Ks,
    device,
    test_batch_size,
    train_user_dict,
    test_user_dict,
    n_items=3148,
):
    model.eval()

    user_ids = list(test_user_dict.keys())
    user_ids_batches = [
        user_ids[i : i + test_batch_size]
        for i in range(0, len(user_ids), test_batch_size)
    ]
    user_ids_batches = [torch.LongTensor(d) for d in user_ids_batches]

    n_items = n_items
    item_ids = torch.arange(n_items, dtype=torch.long).to(device)

    cf_scores = []
    metric_names = ["precision", "recall", "ndcg"]
    metrics_dict = {k: {m: [] for m in metric_names} for k in Ks}

    with tqdm(total=len(user_ids_batches), desc="Evaluating Iteration") as pbar:
        for batch_user_ids in user_ids_batches:
            batch_user_ids = batch_user_ids.to(device)

            with torch.no_grad():
                batch_scores = model(
                    batch_user_ids, item_ids, mode="predict"
                )  # (n_batch_users, n_items)

            print(f"Batch User IDs: {batch_user_ids}")
            print(f"Top 50 batch scores: {batch_scores[:][:50]}")
            batch_scores = batch_scores.cpu()
            batch_metrics = calc_metrics_at_k(
                batch_scores,
                train_user_dict,
                test_user_dict,
                batch_user_ids.cpu().numpy(),
                item_ids.cpu().numpy(),
                Ks,
            )

            cf_scores.append(batch_scores.numpy())
            for k in Ks:
                for m in metric_names:
                    metrics_dict[k][m].append(batch_metrics[k][m])
            pbar.update(1)

    cf_scores = np.concatenate(cf_scores, axis=0)
    for k in Ks:
        for m in metric_names:
            metrics_dict[k][m] = np.concatenate(metrics_dict[k][m]).mean()
    return cf_scores, metrics_dict


def predict(test_batch_size, train_user_dict, test_user_dict):
    # GPU / CPU
    device = torch.device("cuda" if torch.cuda.is_available() else "cpu")

    # load model
    n_users = 70000
    n_entities = 16118
    n_relations = 10
    model = KGAT(
        n_users=n_users, n_entities=n_entities, n_relations=n_relations, use_pretrain=2
    )
    pretrain_model_path = "/kaggle/input/model-first-50-epochs/model_epoch100_new.pth"
    model = load_model(model, pretrain_model_path)
    model.to(device)

    # predict
    Ks = "[20,40,60,80,100]"
    Ks = eval(Ks)
    k_min = min(Ks)
    k_max = max(Ks)

    cf_scores, metrics_dict = evaluate(
        model, Ks, device, test_batch_size, train_user_dict, test_user_dict
    )
    np.save("cf_scores.npy", cf_scores)
    print(
        "CF Evaluation: Precision [{:.4f}, {:.4f}], Recall [{:.4f}, {:.4f}], NDCG [{:.4f}, {:.4f}]".format(
            metrics_dict[k_min]["precision"],
            metrics_dict[k_max]["precision"],
            metrics_dict[k_min]["recall"],
            metrics_dict[k_max]["recall"],
            metrics_dict[k_min]["ndcg"],
            metrics_dict[k_max]["ndcg"],
        )
    )

    # --- LƯU FILE DỰ ĐOÁN ---
    user_ids = list(test_user_dict.keys())  # Đảm bảo đúng thứ tự với cf_scores
    TOP_K = 100  # Có thể thay đổi số lượng top-K tại đây
    predict_file = "predict.csv"

    with open(predict_file, "w", newline="", encoding="utf-8") as f:
        csv_writer = csv.writer(f)
        # --- Ghi header
        header = ["user_id", "ground_truth_item"] + [f"top{i+1}" for i in range(TOP_K)]
        csv_writer.writerow(header)

        # --- Lặp qua từng user đã mapped
        for idx, mapped_user in enumerate(user_ids):
            # 1. Un-map lại user về ID gốc
            orig_user = mapped_user - n_entities

            # 2. Lấy mảng score và chọn TOP_K item
            user_scores = cf_scores[idx]
            top_items = user_scores.argsort()[::-1][
                :TOP_K
            ]  # indices chính là item_id gốc

            # 3. Lấy danh sách ground-truth items (vẫn là ID gốc)
            gt_items = test_user_dict.get(mapped_user, [])

            # 4. Ghi từng dòng: mỗi ground_truth_item thành một row
            for gt_item in gt_items:
                row = [orig_user, gt_item] + top_items.tolist()
            csv_writer.writerow(row)

    print(f"Đã lưu file predict.csv với top-{TOP_K} dự đoán cho từng user.")
